#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import logging
import os
import random
import re
import signal
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from html import unescape
from typing import Iterable, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import aiohttp
from bs4 import BeautifulSoup


LOG = logging.getLogger("crawler")


TRACKING_KEYS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "utm_id", "utm_name", "utm_reader", "utm_viz_id", "utm_pubreferrer",
    "gclid", "fbclid", "msclkid", "igshid", "mc_cid", "mc_eid",
    "ref", "ref_src", "spm", "scm", "cmpid",
}


def utc_ts() -> int:
    return int(time.time())


def utc_iso(ts: Optional[int] = None) -> str:
    ts = utc_ts() if ts is None else ts
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="seconds")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def stable_text(s: str) -> str:
    s = unescape(s or "")
    s = s.replace("\x00", "")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_url(url: str, *, strip_fragment: bool = True, drop_tracking: bool = True) -> str:
    url = url.strip()
    if not url:
        return url

    parts = urlsplit(url)
    scheme = parts.scheme.lower() or "http"
    netloc = parts.netloc.lower()

    if ":" in netloc:
        host, port = netloc.rsplit(":", 1)
        if (scheme == "http" and port == "80") or (scheme == "https" and port == "443"):
            netloc = host

    path = parts.path or "/"

    query_pairs = parse_qsl(parts.query, keep_blank_values=True)

    if drop_tracking and query_pairs:
        filtered = []
        for k, v in query_pairs:
            lk = k.lower()
            if lk in TRACKING_KEYS:
                continue
            if lk.startswith("utm_"):
                continue
            filtered.append((k, v))
        query_pairs = filtered

    query = urlencode(query_pairs, doseq=True)

    fragment = "" if strip_fragment else parts.fragment
    return urlunsplit((scheme, netloc, path, query, fragment))


def same_site(a: str, b: str) -> bool:
    try:
        na = urlsplit(a)
        nb = urlsplit(b)
        return na.netloc.lower() == nb.netloc.lower()
    except Exception:
        return False


def extract_links(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    out: list[str] = []

    for tag in soup.select("a[href]"):
        href = tag.get("href")
        if not href:
            continue
        href = href.strip()
        if href.startswith("#"):
            continue
        if href.startswith("mailto:") or href.startswith("tel:") or href.startswith("javascript:"):
            continue
        abs_url = urljoin(base_url, href)
        out.append(abs_url)

    return out


def extract_title(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    if soup.title and soup.title.string:
        return stable_text(soup.title.string)
    h1 = soup.find("h1")
    if h1 and h1.get_text():
        return stable_text(h1.get_text())
    return ""


_word_re = re.compile(r"[A-Za-z0-9]+", re.UNICODE)


def simhash64(text: str) -> int:
    """
    Simple 64-bit SimHash.
    Good enough near-duplicate detection for now.
    """
    text = stable_text(text).lower()
    words = _word_re.findall(text)
    if not words:
        return 0

    weights = [0] * 64
    for w in words:
        h = hashlib.blake2b(w.encode("utf-8", "ignore"), digest_size=8).digest()
        x = int.from_bytes(h, "big", signed=False)
        for i in range(64):
            bit = (x >> i) & 1
            weights[i] += 1 if bit else -1

    out = 0
    for i, v in enumerate(weights):
        if v >= 0:
            out |= (1 << i)
    return out


def hamming64(a: int, b: int) -> int:
    return (a ^ b).bit_count()


@dataclass(frozen=True)
class FetchResult:
    url: str
    status: int
    final_url: str
    content_type: str
    bytes_len: int
    sha256: str
    simhash: int
    title: str
    error: str


class TokenBucket:
    def __init__(self, rate_per_sec: float, burst: float):
        self.rate = max(0.01, float(rate_per_sec))
        self.burst = max(1.0, float(burst))
        self.tokens = self.burst
        self.updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def take(self, n: float = 1.0):
        async with self._lock:
            while True:
                now = time.monotonic()
                elapsed = now - self.updated
                self.updated = now
                self.tokens = min(self.burst, self.tokens + elapsed * self.rate)

                if self.tokens >= n:
                    self.tokens -= n
                    return

                need = n - self.tokens
                sleep_s = need / self.rate
                sleep_s = max(0.01, sleep_s)
                await asyncio.sleep(sleep_s)


class DB:
    def __init__(self, path: str):
        self.path = path
        self.conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self._init_schema()

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    def _init_schema(self):
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS pages (
                url TEXT PRIMARY KEY,
                fetched_at INTEGER NOT NULL,
                status INTEGER NOT NULL,
                final_url TEXT NOT NULL,
                content_type TEXT NOT NULL,
                bytes_len INTEGER NOT NULL,
                sha256 TEXT NOT NULL,
                simhash INTEGER NOT NULL,
                title TEXT NOT NULL,
                error TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS links (
                from_url TEXT NOT NULL,
                to_url   TEXT NOT NULL,
                PRIMARY KEY (from_url, to_url)
            );

            CREATE INDEX IF NOT EXISTS idx_pages_fetched_at ON pages(fetched_at);
            CREATE INDEX IF NOT EXISTS idx_pages_sha256 ON pages(sha256);
            CREATE INDEX IF NOT EXISTS idx_pages_simhash ON pages(simhash);
            """
        )

    def page_is_fresh(self, url: str, max_age_s: int) -> bool:
        row = self.conn.execute(
            "SELECT fetched_at FROM pages WHERE url = ?;",
            (url,),
        ).fetchone()
        if not row:
            return False
        return (utc_ts() - int(row[0])) <= max_age_s

    def upsert_page(self, r: FetchResult):
        self.conn.execute(
            """
            INSERT INTO pages(url,fetched_at,status,final_url,content_type,bytes_len,sha256,simhash,title,error)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(url) DO UPDATE SET
                fetched_at=excluded.fetched_at,
                status=excluded.status,
                final_url=excluded.final_url,
                content_type=excluded.content_type,
                bytes_len=excluded.bytes_len,
                sha256=excluded.sha256,
                simhash=excluded.simhash,
                title=excluded.title,
                error=excluded.error
            ;
            """,
            (
                r.url,
                utc_ts(),
                r.status,
                r.final_url,
                r.content_type,
                r.bytes_len,
                r.sha256,
                int(r.simhash),
                r.title,
                r.error,
            ),
        )

    def add_links(self, from_url: str, to_urls: Iterable[str]):
        rows = [(from_url, u) for u in to_urls]
        if not rows:
            return
        self.conn.executemany(
            """
            INSERT OR IGNORE INTO links(from_url,to_url) VALUES(?,?);
            """,
            rows,
        )

    def export_pages(self, out_path: str, fmt: str):
        cur = self.conn.execute(
            """
            SELECT url,fetched_at,status,final_url,content_type,bytes_len,sha256,simhash,title,error
            FROM pages
            ORDER BY fetched_at DESC;
            """
        )
        rows = cur.fetchall()

        if fmt == "jsonl":
            with open(out_path, "w", encoding="utf-8") as f:
                for r in rows:
                    obj = {
                        "url": r[0],
                        "fetched_at": utc_iso(int(r[1])),
                        "status": int(r[2]),
                        "final_url": r[3],
                        "content_type": r[4],
                        "bytes_len": int(r[5]),
                        "sha256": r[6],
                        "simhash": int(r[7]),
                        "title": r[8],
                        "error": r[9],
                    }
                    f.write(json.dumps(obj, ensure_ascii=False) + "\n")
            return

        if fmt == "csv":
            with open(out_path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(
                    ["url", "fetched_at", "status", "final_url", "content_type", "bytes_len", "sha256", "simhash", "title", "error"]
                )
                for r in rows:
                    w.writerow(
                        [
                            r[0],
                            utc_iso(int(r[1])),
                            int(r[2]),
                            r[3],
                            r[4],
                            int(r[5]),
                            r[6],
                            int(r[7]),
                            r[8],
                            r[9],
                        ]
                    )
            return

        raise ValueError(f"unknown format: {fmt}")

    def find_near_duplicates(self, max_dist: int = 3, limit: int = 2000) -> list[tuple[str, str, int]]:
        # Hobby-grade: compare within coarse buckets by prefix.
        # This keeps runtime ok for small DBs.
        cur = self.conn.execute(
            """
            SELECT url, simhash
            FROM pages
            WHERE error = ''
            ORDER BY fetched_at DESC
            LIMIT ?;
            """,
            (limit,),
        )
        items = [(u, int(s)) for (u, s) in cur.fetchall()]
        buckets: dict[int, list[tuple[str, int]]] = {}
        for u, s in items:
            key = (s >> 48) & 0xFFFF
            buckets.setdefault(key, []).append((u, s))

        out: list[tuple[str, str, int]] = []
        for _, lst in buckets.items():
            n = len(lst)
            if n < 2:
                continue
            for i in range(n):
                for j in range(i + 1, n):
                    d = hamming64(lst[i][1], lst[j][1])
                    if d <= max_dist:
                        out.append((lst[i][0], lst[j][0], d))
        out.sort(key=lambda x: x[2])
        return out


class Crawl:
    def __init__(
        self,
        db: DB,
        *,
        concurrency: int,
        per_domain_rps: float,
        per_domain_burst: float,
        timeout_s: int,
        max_redirects: int,
        max_pages: int,
        max_depth: int,
        max_age_s: int,
        same_site_only: bool,
        user_agent: str,
        allow_domains: Optional[list[str]],
    ):
        self.db = db
        self.concurrency = max(1, int(concurrency))
        self.per_domain_rps = float(per_domain_rps)
        self.per_domain_burst = float(per_domain_burst)
        self.timeout_s = int(timeout_s)
        self.max_redirects = int(max_redirects)
        self.max_pages = int(max_pages)
        self.max_depth = int(max_depth)
        self.max_age_s = int(max_age_s)
        self.same_site_only = bool(same_site_only)
        self.user_agent = user_agent
        self.allow_domains = [d.lower() for d in (allow_domains or [])]

        self._seen: set[str] = set()
        self._queued: set[str] = set()
        self._buckets: dict[str, TokenBucket] = {}
        self._sem = asyncio.Semaphore(self.concurrency)
        self._stop = asyncio.Event()
        self._fetched_ok = 0
        self._fetched_err = 0

    def stop(self):
        self._stop.set()

    def bucket_for(self, url: str) -> TokenBucket:
        host = urlsplit(url).netloc.lower()
        b = self._buckets.get(host)
        if b is None:
            b = TokenBucket(self.per_domain_rps, self.per_domain_burst)
            self._buckets[host] = b
        return b

    def allowed(self, url: str, seed_netloc: str) -> bool:
        try:
            p = urlsplit(url)
        except Exception:
            return False
        if p.scheme not in ("http", "https"):
            return False
        if not p.netloc:
            return False
        if self.allow_domains:
            host = p.netloc.lower()
            if not any(host == d or host.endswith("." + d) for d in self.allow_domains):
                return False
        if self.same_site_only:
            return p.netloc.lower() == seed_netloc.lower()
        return True

    async def fetch(self, session: aiohttp.ClientSession, url: str) -> FetchResult:
        nurl = normalize_url(url)
        bucket = self.bucket_for(nurl)
        await bucket.take(1.0)

        timeout = aiohttp.ClientTimeout(total=self.timeout_s)
        headers = {"User-Agent": self.user_agent, "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1"}

        try:
            async with self._sem:
                async with session.get(
                    nurl,
                    headers=headers,
                    timeout=timeout,
                    allow_redirects=True,
                    max_redirects=self.max_redirects,
                ) as resp:
                    ct = resp.headers.get("Content-Type", "")
                    final_url = str(resp.url)
                    status = int(resp.status)

                    if "text/html" not in ct.lower():
                        body = await resp.read()
                        return FetchResult(
                            url=nurl,
                            status=status,
                            final_url=normalize_url(final_url),
                            content_type=ct.split(";")[0].strip(),
                            bytes_len=len(body),
                            sha256=sha256_bytes(body),
                            simhash=0,
                            title="",
                            error="non-html",
                        )

                    body = await resp.read()
                    if not body:
                        return FetchResult(
                            url=nurl,
                            status=status,
                            final_url=normalize_url(final_url),
                            content_type="text/html",
                            bytes_len=0,
                            sha256=sha256_bytes(b""),
                            simhash=0,
                            title="",
                            error="empty",
                        )

                    # try to decode with aiohttp hints
                    text = None
                    try:
                        text = await resp.text(errors="ignore")
                    except Exception:
                        text = body.decode("utf-8", "ignore")

                    title = extract_title(text)
                    soup = BeautifulSoup(text, "html.parser")
                    page_text = soup.get_text(" ", strip=True)
                    sh = simhash64(page_text)

                    return FetchResult(
                        url=nurl,
                        status=status,
                        final_url=normalize_url(final_url),
                        content_type="text/html",
                        bytes_len=len(body),
                        sha256=sha256_bytes(body),
                        simhash=sh,
                        title=title,
                        error="",
                    )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            return FetchResult(
                url=nurl,
                status=0,
                final_url=nurl,
                content_type="",
                bytes_len=0,
                sha256="",
                simhash=0,
                title="",
                error=type(e).__name__,
            )

    async def worker(
        self,
        session: aiohttp.ClientSession,
        q: "asyncio.Queue[tuple[str,int,str]]",
    ):
        while not self._stop.is_set():
            try:
                url, depth, seed_netloc = await asyncio.wait_for(q.get(), timeout=0.25)
            except asyncio.TimeoutError:
                if q.empty():
                    return
                continue

            try:
                if url in self._seen:
                    continue
                self._seen.add(url)

                if self.max_age_s > 0 and self.db.page_is_fresh(url, self.max_age_s):
                    continue

                r = await self.fetch(session, url)
                self.db.upsert_page(r)

                if r.error:
                    self._fetched_err += 1
                else:
                    self._fetched_ok += 1

                if (self._fetched_ok + self._fetched_err) % 25 == 0:
                    LOG.info("pages=%d ok=%d err=%d q=%d",
                             self._fetched_ok + self._fetched_err, self._fetched_ok, self._fetched_err, q.qsize())

                if r.error or depth >= self.max_depth or self._stop.is_set():
                    continue

                # link extraction only for html
                # (we stored simhash based on text extraction above)
                # Fetch again? no. Use final_url for base, but we didn't keep html.
                # For 1-file hobby tradeoff: re-fetch cheap HEAD? no.
                # Instead: do a small extra GET for link discovery only when status is 2xx.
                if not (200 <= r.status < 300):
                    continue

                # Lightweight second pass for links with a smaller timeout
                try:
                    timeout = aiohttp.ClientTimeout(total=min(self.timeout_s, 10))
                    headers = {"User-Agent": self.user_agent, "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1"}
                    await self.bucket_for(r.final_url).take(1.0)
                    async with session.get(r.final_url, headers=headers, timeout=timeout, allow_redirects=True) as resp2:
                        ct2 = resp2.headers.get("Content-Type", "")
                        if "text/html" not in ct2.lower():
                            continue
                        html = await resp2.text(errors="ignore")
                except Exception:
                    continue

                raw_links = extract_links(html, r.final_url)
                norm_links = []
                for lk in raw_links:
                    nl = normalize_url(lk)
                    if not self.allowed(nl, seed_netloc):
                        continue
                    norm_links.append(nl)

                if norm_links:
                    self.db.add_links(url, norm_links)

                for nl in norm_links:
                    if self._stop.is_set():
                        break
                    if nl in self._seen or nl in self._queued:
                        continue
                    if len(self._seen) + q.qsize() >= self.max_pages:
                        self._stop.set()
                        break
                    self._queued.add(nl)
                    q.put_nowait((nl, depth + 1, seed_netloc))

            finally:
                q.task_done()

    async def run(self, seeds: list[str]):
        seeds = [normalize_url(s) for s in seeds if s.strip()]
        if not seeds:
            raise SystemExit("no seeds")

        seed_netloc = urlsplit(seeds[0]).netloc.lower()

        q: "asyncio.Queue[tuple[str,int,str]]" = asyncio.Queue()
        for s in seeds:
            if self.allowed(s, seed_netloc):
                self._queued.add(s)
                q.put_nowait((s, 0, seed_netloc))

        connector = aiohttp.TCPConnector(
            limit=self.concurrency * 2,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )

        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [asyncio.create_task(self.worker(session, q)) for _ in range(self.concurrency)]

            await q.join()
            self._stop.set()

            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        LOG.info("done pages=%d ok=%d err=%d", self._fetched_ok + self._fetched_err, self._fetched_ok, self._fetched_err)


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="crawler.py", add_help=True)
    sub = p.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("crawl", help="crawl and cache into sqlite")
    c.add_argument("seeds", nargs="+", help="seed URLs")
    c.add_argument("--db", default="crawl.sqlite3")
    c.add_argument("--concurrency", type=int, default=20)
    c.add_argument("--per-domain-rps", type=float, default=1.5)
    c.add_argument("--per-domain-burst", type=float, default=3.0)
    c.add_argument("--timeout", type=int, default=25)
    c.add_argument("--max-redirects", type=int, default=8)
    c.add_argument("--max-pages", type=int, default=1500)
    c.add_argument("--max-depth", type=int, default=2)
    c.add_argument("--max-age", type=int, default=24 * 3600, help="skip cached pages fresher than N seconds (0 disables)")
    c.add_argument("--allow-domain", action="append", default=[], help="restrict to these domains (repeatable)")
    c.add_argument("--cross-site", action="store_true", help="allow following links to other hosts")
    c.add_argument("--ua", default="Mozilla/5.0 (compatible; hobby-crawler/0.1)")
    c.add_argument("--log", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    e = sub.add_parser("export", help="export cached pages")
    e.add_argument("--db", default="crawl.sqlite3")
    e.add_argument("--out", required=True)
    e.add_argument("--fmt", choices=["csv", "jsonl"], default="jsonl")
    e.add_argument("--near-dupes", action="store_true")
    e.add_argument("--max-dist", type=int, default=3)
    e.add_argument("--limit", type=int, default=2000)

    return p.parse_args(argv)


async def main_async(ns: argparse.Namespace) -> int:
    if ns.cmd == "crawl":
        logging.basicConfig(
            level=getattr(logging, ns.log),
            format="%(asctime)s %(levelname)s %(message)s",
            datefmt="%H:%M:%S",
        )

        db = DB(ns.db)
        try:
            crawler = Crawl(
                db,
                concurrency=ns.concurrency,
                per_domain_rps=ns.per_domain_rps,
                per_domain_burst=ns.per_domain_burst,
                timeout_s=ns.timeout,
                max_redirects=ns.max_redirects,
                max_pages=ns.max_pages,
                max_depth=ns.max_depth,
                max_age_s=ns.max_age,
                same_site_only=not ns.cross_site,
                user_agent=ns.ua,
                allow_domains=ns.allow_domain,
            )

            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, crawler.stop)
                except NotImplementedError:
                    pass

            await crawler.run(ns.seeds)
            return 0
        finally:
            db.close()

    if ns.cmd == "export":
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
            datefmt="%H:%M:%S",
        )
        db = DB(ns.db)
        try:
            db.export_pages(ns.out, ns.fmt)
            LOG.info("exported %s (%s)", ns.out, ns.fmt)
            if ns.near_dupes:
                pairs = db.find_near_duplicates(max_dist=ns.max_dist, limit=ns.limit)
                out2 = os.path.splitext(ns.out)[0] + ".near_dupes.jsonl"
                with open(out2, "w", encoding="utf-8") as f:
                    for a, b, d in pairs:
                        f.write(json.dumps({"a": a, "b": b, "dist": d}, ensure_ascii=False) + "\n")
                LOG.info("near-dupes %d -> %s", len(pairs), out2)
            return 0
        finally:
            db.close()

    return 2


def main(argv: list[str]) -> int:
    ns = parse_args(argv)
    try:
        return asyncio.run(main_async(ns))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
