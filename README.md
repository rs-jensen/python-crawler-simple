# Async Web Crawler (single-file)

This is a small but serious hobby crawler written in one Python file.

It crawls web pages asynchronously, stores results in SQLite, detects near-duplicate pages, and exports data to CSV or JSONL.  
The goal is not scale at any cost, but correctness, clarity, and realistic behavior.

No external services. No framework. One file.

---

## Features

- Async crawling with bounded concurrency
- Per-domain rate limiting
- URL normalization and tracking parameter removal
- HTML title and text extraction
- Content hashing (SHA-256)
- Near-duplicate detection using SimHash
- Persistent SQLite cache with resume support
- Link graph storage
- CSV and JSONL export
- Graceful shutdown on SIGINT

---

## What this is

- A realistic crawler you could extend
- A reference project for async I/O, state, and persistence
- A codebase written to be read, not just to run

## What this is not

- A full scraping framework
- A distributed system
- An attempt to bypass site protections

---

## Requirements

- Python 3.10+
- sqlite3 (built-in)
- aiohttp
- beautifulsoup4

Install dependencies:

```bash
pip install aiohttp beautifulsoup4
