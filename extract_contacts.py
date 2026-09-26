"""
Sellers.json contact email extractor.

Reads URLs from exchanges.txt, fetches each sellers.json asynchronously,
extracts the root-level `contact_email`, sanitizes and deduplicates,
then streams unique results to contacts.txt.
"""

import asyncio
import json
import logging
import re
import ssl
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import aiohttp
from aiohttp import ClientTimeout, TCPConnector
from tqdm.asyncio import tqdm

INPUT_FILE = Path("exchanges.txt")
OUTPUT_FILE = Path("contacts.txt")

CONCURRENCY = 100
DNS_THREADS = 300  # >> CONCURRENCY so dead-domain getaddrinfo() calls can't starve us
MAX_BYTES = 5 * 1024 * 1024  # 5 MB cap per URL
TIMEOUT_SECONDS = 10
QUEUE_SENTINEL = None

USER_AGENT = (
    "Mozilla/5.0 (compatible; SellersJsonExtractor/1.0; "
    "+https://iabtechlab.com/sellers-json/)"
)

# Strict-enough check to filter out garbage that happens to contain '@'.
EMAIL_RE = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")


def _silence_noisy_loggers() -> None:
    """Mute the asyncio/aiohttp transport chatter so the console stays clean."""
    for name in ("asyncio", "aiohttp", "aiohttp.client", "aiohttp.connector"):
        logging.getLogger(name).setLevel(logging.CRITICAL)

    def _swallow(loop, context):
        # Drop SSL/transport noise that asyncio surfaces via the default handler.
        return

    try:
        asyncio.get_event_loop().set_exception_handler(_swallow)
    except RuntimeError:
        pass


def sanitize_email(raw: object) -> str | None:
    """Normalize an email-ish value; return None if it doesn't look like one."""
    if not isinstance(raw, str):
        return None
    email = raw.strip().lower()
    if email.startswith("mailto:"):
        email = email[len("mailto:"):].strip()
    # Strip trailing punctuation / angle brackets that occasionally creep in.
    email = email.strip("<>\"' \t\r\n,;")
    if not email or len(email) < 5 or len(email) > 254:
        return None
    if not EMAIL_RE.match(email):
        return None
    return email


def load_urls(path: Path) -> list[str]:
    urls: list[str] = []
    with path.open("r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            url = line.strip()
            if url and not url.startswith("#"):
                urls.append(url)
    return urls


async def fetch_email(session: aiohttp.ClientSession, url: str) -> str | None:
    """Download up to MAX_BYTES, parse JSON, return sanitized contact_email."""
    try:
        async with session.get(url, allow_redirects=True) as resp:
            if resp.status != 200:
                return None

            # Read in chunks so we honor the size cap even when the server
            # lies about Content-Length (or omits it entirely).
            buf = bytearray()
            async for chunk in resp.content.iter_chunked(64 * 1024):
                buf.extend(chunk)
                if len(buf) >= MAX_BYTES:
                    break

        # Best-effort decode. sellers.json should be UTF-8 but tolerate junk;
        # errors="replace" guarantees we never raise on weird byte sequences.
        text = buf.decode("utf-8", errors="replace").lstrip("﻿").strip()
        if not text:
            return None

        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            # Truncated payloads (because we capped MAX_BYTES) land here too.
            # contact_email lives at the root, so it's almost always already
            # in `text` — try a forgiving second pass before giving up.
            return _salvage_contact_email(text)

        if not isinstance(data, dict):
            return None
        return sanitize_email(data.get("contact_email"))

    except (
        aiohttp.ClientError,
        asyncio.TimeoutError,
        ssl.SSLError,
        UnicodeError,
        ConnectionError,
        OSError,
    ):
        return None
    except Exception:
        # Last-ditch swallow — we don't want one bad URL to kill the worker.
        return None


def _salvage_contact_email(text: str) -> str | None:
    """Pull `contact_email` out of a truncated/malformed JSON blob."""
    needle = '"contact_email"'
    idx = text.find(needle)
    if idx == -1:
        return None
    # Find the value delimited by the next pair of double quotes after the colon.
    colon = text.find(":", idx + len(needle))
    if colon == -1:
        return None
    start_q = text.find('"', colon + 1)
    if start_q == -1:
        return None
    end_q = text.find('"', start_q + 1)
    if end_q == -1:
        return None
    return sanitize_email(text[start_q + 1:end_q])


async def worker(
    queue: asyncio.Queue,
    session: aiohttp.ClientSession,
    writer_queue: asyncio.Queue,
    pbar: tqdm,
) -> None:
    while True:
        url = await queue.get()
        try:
            if url is QUEUE_SENTINEL:
                return
            email = await fetch_email(session, url)
            if email:
                await writer_queue.put(email)
        finally:
            pbar.update(1)
            queue.task_done()


async def writer_task(
    writer_queue: asyncio.Queue,
    output_path: Path,
    seen: set[str],
) -> int:
    """Stream unique emails to disk, flushing after every write."""
    written = 0
    # Append mode so reruns extend the file; switch to "w" for a fresh run.
    with output_path.open("a", encoding="utf-8", buffering=1) as out:
        while True:
            item = await writer_queue.get()
            try:
                if item is QUEUE_SENTINEL:
                    return written
                if item in seen:
                    continue
                seen.add(item)
                out.write(item + "\n")
                out.flush()
                written += 1
            finally:
                writer_queue.task_done()


def load_existing(output_path: Path) -> set[str]:
    if not output_path.exists():
        return set()
    seen: set[str] = set()
    with output_path.open("r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            e = line.strip().lower()
            if e:
                seen.add(e)
    return seen


async def main() -> None:
    _silence_noisy_loggers()

    # aiohttp resolves hostnames via loop.getaddrinfo(), which dispatches to the
    # default ThreadPoolExecutor (min(32, cpu+4) threads). With 100 concurrent
    # workers hammering dead/slow domains, that pool deadlocks quickly. Override
    # with a fat pool sized well above CONCURRENCY so DNS never becomes the
    # bottleneck.
    loop = asyncio.get_running_loop()
    dns_pool = ThreadPoolExecutor(
        max_workers=DNS_THREADS, thread_name_prefix="dns"
    )
    loop.set_default_executor(dns_pool)

    if not INPUT_FILE.exists():
        print(f"[!] Input file not found: {INPUT_FILE}", file=sys.stderr)
        sys.exit(1)

    urls = load_urls(INPUT_FILE)
    if not urls:
        print("[!] No URLs to process.", file=sys.stderr)
        sys.exit(0)

    seen_emails: set[str] = load_existing(OUTPUT_FILE)
    print(f"[i] {len(urls):,} URLs queued | {len(seen_emails):,} emails already on disk")

    queue: asyncio.Queue = asyncio.Queue()
    writer_queue: asyncio.Queue = asyncio.Queue()
    for url in urls:
        queue.put_nowait(url)

    timeout = ClientTimeout(total=TIMEOUT_SECONDS, connect=TIMEOUT_SECONDS)
    connector = TCPConnector(
        limit=CONCURRENCY,
        limit_per_host=8,
        ssl=False,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json,text/plain,*/*"}

    pbar = tqdm(total=len(urls), unit="url", smoothing=0.05, dynamic_ncols=True)

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        headers=headers,
        trust_env=True,
    ) as session:
        writer = asyncio.create_task(writer_task(writer_queue, OUTPUT_FILE, seen_emails))

        workers = [
            asyncio.create_task(worker(queue, session, writer_queue, pbar))
            for _ in range(CONCURRENCY)
        ]

        # Tell each worker to exit after the queue drains.
        for _ in range(CONCURRENCY):
            queue.put_nowait(QUEUE_SENTINEL)

        await asyncio.gather(*workers, return_exceptions=True)

        # Drain whatever the workers wrote, then shut the writer down.
        await writer_queue.join()
        await writer_queue.put(QUEUE_SENTINEL)
        written = await writer

    pbar.close()
    print(f"[✓] Done. {written:,} new unique emails written to {OUTPUT_FILE}")
    print(f"[✓] Total unique emails on disk: {len(seen_emails):,}")

    dns_pool.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[!] Interrupted by user.", file=sys.stderr)
