# async-sellers-json-contact-extractor

**Asynchronously harvest, sanitize, and deduplicate `contact_email` addresses from thousands of ad-tech [`sellers.json`](https://iabtechlab.com/sellers-json/) files — at scale, in seconds.**

[![License: Apache--2.0](https://img.shields.io/badge/License-Apache--2.0-blue?style=for-the-badge)](LICENSE)
[![Python: 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/downloads/)
[![Version: 1.0.0](https://img.shields.io/badge/version-1.0.0-informational?style=for-the-badge)](extract_contacts.py)
[![aiohttp: async](https://img.shields.io/badge/aiohttp-async%20I%2FO-e57000?style=for-the-badge)](https://docs.aiohttp.org/)
[![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20macOS%20%7C%20Windows-lightgrey?style=for-the-badge)](#getting-started)
[![Last Commit](https://img.shields.io/github/last-commit/adops-tool/async-sellers-json-contact-extractor?style=for-the-badge)](https://github.com/adops-tool/async-sellers-json-contact-extractor/commits)
[![PRs: Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen?style=for-the-badge)](https://github.com/adops-tool/async-sellers-json-contact-extractor/pulls)

> [!NOTE]
> [`sellers.json`](https://iabtechlab.com/sellers-json/) is the IAB Tech Lab transparency standard that lets sellers (publishers, intermediaries) declare their inventory sellers and a root-level `contact_email` for business inquiries. This tool turns a raw list of `sellers.json` endpoints into a clean, deduplicated mailing list — the daily bread-and-butter workflow of ad-ops and business-development teams.

---

## Table of Contents

- [Features](#features)
- [Tech Stack & Architecture](#tech-stack--architecture)
  - [Core Technologies](#core-technologies)
  - [Project Structure](#project-structure)
  - [Key Design Decisions](#key-design-decisions)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Troubleshooting](#troubleshooting)
- [Testing](#testing)
- [Deployment](#deployment)
- [Usage](#usage)
  - [Basic Usage](#basic-usage)
  - [Advanced Usage](#advanced-usage)
  - [Edge Cases & Deep Dives](#edge-cases--deep-dives)
- [Configuration](#configuration)
  - [Module Constants](#module-constants)
  - [Input & Output Formats](#input--output-formats)
  - [Environment Variables](#environment-variables)
- [License](#license)
- [Contacts & Community Support](#contacts--community-support)
  - [Support the Project](#support-the-project)

---

## Features

- **Massively asynchronous fetch pipeline** — built on `asyncio` + `aiohttp` with a configurable worker pool (default **100 concurrent fetches**) for maximum throughput over slow or unreliable endpoints.
- **DNS-starvation protection** — a dedicated `ThreadPoolExecutor` of **300 resolver threads** backs `getaddrinfo()`, so dead domains can never deadlock the event loop (see [Key Design Decisions](#key-design-decisions)).
- **Streaming, chunked downloads with a hard size cap** — responses are read in 64 KiB chunks and truncated at **5 MiB per URL**, honoring the cap even when servers lie about `Content-Length`.
- **Strict-then-forgiving JSON parsing** — full `json.loads()` first; on failure a **salvage pass** recovers `contact_email` from truncated/malformed payloads (common when the 5 MiB cap cuts off a huge `sellers` array — the root-level field is almost always already in the buffer).
- **Robust email sanitization** — lowercases, strips whitespace and stray `<>\"' ,;` punctuation, removes `mailto:` prefixes, enforces a 5–254 character bound, and validates against a conservative regex so garbage containing `@` never pollutes the output.
- **Crash-safe, resumable output** — emails are streamed to `contacts.txt` in append mode with line-buffered I/O and a `flush()` after every write; an interrupt mid-run loses nothing already written.
- **Idempotent reruns** — the output file is pre-loaded into an in-memory `seen` set, so re-running the tool only appends *new* addresses and never duplicates existing ones.
- **Concurrent-safe single-writer architecture** — workers push to a writer queue consumed by one dedicated writer coroutine, so no locks are required and disk I/O never blocks fetching.
- **Tolerant transport layer** — follows redirects, accepts `http://` and `https://`, disables certificate verification for maximum reach across sketchy ad-tech infrastructure, and honors corporate proxy settings from the environment (`trust_env=True`).
- **Charset resilience** — decodes with `utf-8` + `errors="replace"` and strips a UTF-8 BOM, so non-conforming payloads cannot raise.
- **Silent-failure policy** — timeouts, DNS failures, TLS errors, 4xx/5xx responses, and parse errors are swallowed per-URL; one bad endpoint can never kill a run of thousands.
- **Live progress reporting** — a `tqdm` progress bar (unit = URL, smoothed, auto-resizing) with transport/asyncio log chatter muted for a clean console.
- **Graceful interruption** — `KeyboardInterrupt` is handled explicitly; partial results remain on disk and the exit is clean.
- **Zero-build, single-module footprint** — one ~280-line Python file, two runtime dependencies, no packaging ceremony.
- **Comment-friendly input format** — `exchanges.txt` accepts `#` comments and blank lines, so lists can be annotated and curated in version control.

---

## Tech Stack & Architecture

### Core Technologies

| Layer | Technology | Version | Role |
|---|---|---|---|
| Language | **Python** | **3.10+** (3.11+ recommended) | Runtime; PEP 604 (`str \| None`) syntax requires ≥ 3.10 |
| Async runtime | **`asyncio`](https://docs.python.org/3/library/asyncio.html)** | stdlib | Event loop, queues, task orchestration, sentinel shutdown |
| HTTP client | **[`aiohttp`](https://docs.aiohttp.org/)** | ≥ 3.8 | Non-blocking HTTP/1.1 client with connection pooling |
| Progress UI | **[`tqdm`](https://github.com/tqdm/tqdm)** (`tqdm.asyncio`) | ≥ 4.60 | Live progress bar |
| Concurrency support | **`concurrent.futures.ThreadPoolExecutor`** | stdlib | Oversized DNS resolver pool (300 threads) |
| Parsing | **`json`**, **`re`** | stdlib | Strict JSON decode + salvage regex + email validation |
| Transport | **`ssl`**, **`pathlib`**, **`logging`** | stdlib | TLS error taxonomy, file I/O, log silencing |

**Runtime dependencies (exactly two):** `aiohttp`, `tqdm`. Everything else is the Python standard library.

### Project Structure

<details>
<summary><b>📁 Full repository file tree</b> (click to expand)</summary>

```text
async-sellers-json-contact-extractor/
├── LICENSE                 # Apache License 2.0 (full legal text)
├── README.md               # This document — complete technical reference
├── cmd_commands.txt        # Two-line quickstart cheat sheet
│                             (pip install aiohttp tqdm / python extract_contacts.py)
├── contacts.txt            # OUTPUT: harvested, deduplicated contact emails
│                             (one lowercase address per line, ~627 shipped snapshot)
├── exchanges.txt           # INPUT: curated sellers.json endpoint URLs
│                             (one URL per line, ~1,308 entries shipped snapshot)
└── extract_contacts.py     # CORE MODULE & CLI ENTRY POINT (~280 lines)
                              ├── Constants & tuning knobs (CONCURRENCY, MAX_BYTES, …)
                              ├── EMAIL_RE — conservative email validator
                              ├── _silence_noisy_loggers() — mutes asyncio/aiohttp chatter
                              ├── sanitize_email() — normalize + validate an address
                              ├── load_urls() — parse exchanges.txt (skips #comments/blanks)
                              ├── fetch_email() — chunked, capped download + parse + salvage
                              ├── _salvage_contact_email() — regex recovery from bad JSON
                              ├── worker() — queue consumer (fetch → writer_queue)
                              ├── writer_task() — single-writer dedup + streaming flush
                              ├── load_existing() — preload seen set from contacts.txt
                              └── main() — orchestrator: DNS pool, session, workers, shutdown
```

</details>

### Key Design Decisions

1. **Single-module CLI that doubles as a library.** `extract_contacts.py` is importable (`sanitize_email`, `load_urls`, `fetch_email`, …) *and* runnable as a script. There is no packaging boilerplate to maintain, and consumers can cherry-pick functions into their own pipelines.

2. **Producer/consumer fan-out with one writer.** N fetch workers (`CONCURRENCY`, default 100) share a single `aiohttp.ClientSession` and push results onto a `writer_queue` drained by one coroutine. This gives lock-free deduplication and keeps disk I/O completely off the hot network path.

3. **Oversized DNS executor on purpose.** `aiohttp` resolves hostnames via `loop.getaddrinfo()`, which dispatches to the loop's default `ThreadPoolExecutor` — typically only `min(32, cpu + 4)` threads. With 100 workers hammering dead domains, that pool deadlocks quickly. The tool replaces it with a **300-thread** pool (`DNS_THREADS >> CONCURRENCY`) so resolution can never starve fetching.

4. **Sentinel-based clean shutdown.** Each worker receives a `QUEUE_SENTINEL` after the real work is enqueued; workers exit on receipt, then the writer is drained, joined, and sent its own sentinel. No cancellation storms, no orphaned tasks.

5. **Defense-in-depth parsing.** Strict `json.loads()` first, then `_salvage_contact_email()` — a quote-delimited scan for `"contact_email"` — because truncated responses (cut at `MAX_BYTES`) still almost always contain the root-level field.

6. **Per-line flush + append mode = resumability.** Combined with the pre-loaded `seen` set, this makes runs idempotent and interruptions cheap: rerun the exact same command to continue where you left off.

7. **Silent-failure policy over fail-fast.** The corpus of ad-tech endpoints is full of dead domains, expired certs, 404s, and HTML error pages pretending to be JSON. Every per-URL failure mode maps to `None` and the run continues; only a missing `exchanges.txt` is fatal (exit code 1).

8. **`ssl=False` and `trust_env=True`.** Certificate verification is disabled to maximize harvest yield across misconfigured ad-tech hosts (see [Configuration](#configuration) for the security trade-off), and proxy variables are honored for corporate environments.

<details>
<summary><b>🔀 Architecture & data-flow diagram (Mermaid)</b> (click to expand)</summary>

```mermaid
flowchart TD
    A[("exchanges.txt<br/>seed URL list")] -->|"load_urls(): strip,<br/>skip #comments & blanks"| B[("asyncio.Queue<br/>URL queue")]
    B --> W["Worker pool<br/>CONCURRENCY = 100 tasks"]
    W --> F["aiohttp GET (shared ClientSession)<br/>64 KiB chunks · 5 MiB cap · 10 s timeout<br/>redirects on · ssl=False · trust_env"]
    F --> P{"json.loads() OK?"}
    P -->|"yes (well-formed)"| E["data.get('contact_email')"]
    P -->|"no (truncated / malformed)"| S["_salvage_contact_email()<br/>quote-delimited scan"]
    S --> E
    E --> V["sanitize_email()<br/>lowercase · mailto: strip<br/>length 5–254 · EMAIL_RE"]
    V -->|"invalid"| D["discard"]
    V -->|"valid"| Q[("writer_queue")]
    Q --> DU{"seen set<br/>duplicate?"}
    DU -->|"yes"| D
    DU -->|"no"| O[("contacts.txt<br/>append · line-buffered · flush()")]
    W -.->|"QUEUE_SENTINEL per worker<br/>→ clean shutdown"| W
    O -.->|"load_existing() on startup<br/>→ idempotent reruns"| DU
```

**Shutdown sequence:** enqueue N sentinels → `gather()` workers → `writer_queue.join()` → writer sentinel → await writer → close `tqdm` → `dns_pool.shutdown(cancel_futures=True)`.

</details>

---

## Getting Started

### Prerequisites

| Requirement | Minimum | Recommended | Notes |
|---|---|---|---|
| **Python** | **3.10** | 3.11 or newer | Hard floor: the source uses PEP 604 union syntax (`str \| None`), which errors on 3.9 and below |
| **pip** | 21.x | latest | `python -m pip install --upgrade pip` if in doubt |
| **Network access** | outbound HTTP/HTTPS | — | Direct or via `HTTP(S)_PROXY` environment variables |
| **File-descriptor limit** | ~256 | ≥ 4096 | 100 concurrent sockets + DNS threads; see [Troubleshooting](#troubleshooting) |
| **Disk space** | a few MB | — | Output is one line per unique email |
| **OS** | Linux, macOS, Windows | Linux | Pure-Python; no native extensions |

### Installation

```bash
# 1. Clone the repository
git clone https://github.com/adops-tool/async-sellers-json-contact-extractor.git
cd async-sellers-json-contact-extractor

# 2. (Recommended) create and activate an isolated virtual environment
python3 -m venv .venv
source .venv/bin/activate          # Windows (cmd): .venv\Scripts\activate.bat
                                   # Windows (PowerShell): .venv\Scripts\Activate.ps1

# 3. Install the two runtime dependencies
pip install aiohttp tqdm

# 4. Run it — that's the entire build process
python extract_contacts.py
```

> [!TIP]
> The repository ships a `cmd_commands.txt` cheat sheet containing the two essential commands (`pip install aiohttp tqdm` → `python extract_contacts.py`) for when you just want to get running fast.

### Troubleshooting

<details>
<summary><b>🔧 Alternative installation methods</b> (click to expand)</summary>

**Pinned / reproducible installs**

```bash
# Pin exact versions in a requirements.txt, then:
pip install -r requirements.txt
# Suggested pins:
#   aiohttp>=3.8,<4
#   tqdm>=4.60,<5
```

**Install for the current user only (no venv, no root)**

```bash
pip install --user aiohttp tqdm
python extract_contacts.py
```

**Building from source / offline machines**

The project has *no build step* — it is a pure-Python single file. On an air-gapped host, download the `aiohttp` and `tqdm` wheels on a connected machine (`pip download aiohttp tqdm -d wheels/`), copy the `wheels/` directory across, and install offline:

```bash
pip install --no-index --find-links wheels/ aiohttp tqdm
```

</details>

<details>
<summary><b>🧰 Common problems & fixes</b> (click to expand)</summary>

**Common problems**

| Symptom | Cause | Fix |
|---|---|---|
| `SyntaxError` / `TypeError: unsupported operand` at import on startup | Python ≤ 3.9 parsing `str \| None` | Use Python 3.10+ (`python3 --version`) |
| `ModuleNotFoundError: No module named 'aiohttp'` | Dependencies installed into a different interpreter | Use `python -m pip install aiohttp tqdm` (same interpreter you run with), or activate your venv |
| `OSError: [Errno 24] Too many open files` | Default `ulimit -n` too low for 100 sockets | `ulimit -n 4096` (session) or raise `LimitNOFILE` in systemd |
| `[!] Input file not found: exchanges.txt` | Wrong working directory | Run from the directory containing `exchanges.txt`, or edit `INPUT_FILE` |
| `[!] No URLs to process.` | Every line is blank or a `#` comment | Populate `exchanges.txt` with one URL per line |
| Run appears frozen on first bar frame | DNS resolution stalling (dead domains) | Expected briefly; the 300-thread DNS pool absorbs it. Lower `CONCURRENCY` on very constrained hosts |
| Corporate proxy returns `407`/connection refused | Proxy not exported to the environment | `export HTTPS_PROXY=http://user:pass@proxy:3128` (`trust_env=True` picks it up) |
| Windows event-loop warnings | Legacy `Proactor`/`Selector` policy quirks on old Python | Use Python 3.11+; the tool runs fine under the default policy |

</details>

---

## Testing

The extractor is designed to be verifiable in seconds. The commands below cover syntax validation, linting, type checking, and an end-to-end smoke test against local fixtures.

```bash
# 1. Syntax / bytecode compilation check (zero dependencies required)
python -m py_compile extract_contacts.py

# 2. Lint (pick your weapon; all are safe, non-destructive checks)
pip install ruff flake8 mypy
ruff check extract_contacts.py                 # fast, modern linter
flake8 extract_contacts.py --max-line-length=120
mypy extract_contacts.py                       # static type check (PEP 604 annotations)

# 3. Unit tests (pytest) — run once a tests/ directory is present
pytest tests/ -q
```

> [!NOTE]
> The repository currently ships **without a formal unit-test suite or CI workflow** — the commands above are the recommended verification gate for contributors. The [Deployment](#deployment) section includes a ready-to-paste GitHub Actions workflow that runs these checks on every push. New tests (especially around `sanitize_email`, `_salvage_contact_email`, and `load_urls`) are very welcome via pull request.

**Integration smoke test** — the fastest way to prove the whole pipeline works is to run the real script against local fixtures:

<details>
<summary><b>🧪 Reproducible end-to-end smoke test (verified)</b> (click to expand)</summary>

```bash
mkdir -p /tmp/smoke/fixtures/{good,trunc,mailto}
cd /tmp/smoke

# --- Fixtures: one per behavior under test ---
printf '{"contact_email": "Sales@Example.com ", "sellers": []}'      > fixtures/good/sellers.json     # happy path + case/whitespace normalization
printf '{"contact_email": "ops@example.org", "sellers": [{"id": "1"' > fixtures/trunc/sellers.json   # truncated JSON → salvage path
printf '{"contact_email": "mailto:hello@example.net"}'               > fixtures/mailto/sellers.json  # mailto: prefix stripping
printf 'hello world, not json at all'                                > fixtures/notjson.txt          # non-JSON → skipped
printf '{"contact_email": 12345}'                                    > fixtures/nonstr.json          # non-string field → skipped

# --- Serve fixtures locally ---
python -m http.server 8077 --bind 127.0.0.1 &   # run from the fixtures/ directory
# (a missing URL below exercises the 404 → skip path)

# --- Input list: comments + blanks must be ignored ---
cat > exchanges.txt <<'EOF'
# comment line — must be ignored

http://127.0.0.1:8077/good/sellers.json
http://127.0.0.1:8077/trunc/sellers.json
http://127.0.0.1:8077/mailto/sellers.json
http://127.0.0.1:8077/notjson.txt
http://127.0.0.1:8077/nonstr.json
http://127.0.0.1:8077/missing/sellers.json
EOF

cp /path/to/repo/extract_contacts.py .
python extract_contacts.py        # run #1 → 3 new emails
python extract_contacts.py        # run #2 → 0 new (dedupe/idempotency)
```

**Verified output:**

```text
[i] 6 URLs queued | 0 emails already on disk
[✓] Done. 3 new unique emails written to contacts.txt
[✓] Total unique emails on disk: 3
```

```text
# contacts.txt
hello@example.net
ops@example.org
sales@example.com
```

Second run reports `0 new unique emails written` with `3 emails already on disk` — confirming append-mode deduplication. The six input URLs resolve to exactly three valid emails: sanitization (`Sales@Example.com ` → `sales@example.com`), salvage (`trunc`), and `mailto:` stripping (`hello@example.net`) all behave as documented; junk, non-strings, and 404s are silently skipped.

</details>

---

## Deployment

This is a batch CLI tool, so "deployment" means running it **unattended, reproducibly, and on a schedule**. At minimum, deploy it anywhere Python 3.10+ is available, run it from the directory containing `exchanges.txt`, and persist `contacts.txt` (the tool appends to it across runs).

**Production checklist**

1. **Pin dependencies** (`aiohttp>=3.8,<4`, `tqdm>=4.60,<5`) or bake an image (below) for byte-reproducible runs.
2. **Persist the output file** — it doubles as the dedupe state. Mount it as a volume (Docker) or place it on persistent storage (cron/systemd).
3. **Schedule periodic re-harvests** — ad-tech endpoints churn constantly; a nightly or weekly run keeps the list fresh and (thanks to dedupe) strictly growing.
4. **Respect infrastructure limits** — keep the per-host connection cap (`limit_per_host=8`) unless you own both ends; lower `CONCURRENCY` if your egress IP is rate-limited.
5. **Egress through a proxy** where required — `trust_env=True` honors `HTTP_PROXY` / `HTTPS_PROXY` / `NO_PROXY` without code changes.

<details>
<summary><b>🐳 Containerization — Dockerfile</b> (click to expand)</summary>

```dockerfile
# syntax=docker/dockerfile:1
FROM python:3.12-slim

# Two runtime deps only — layer caches beautifully
RUN pip install --no-cache-dir "aiohttp>=3.8,<4" "tqdm>=4.60,<5"

WORKDIR /app
COPY extract_contacts.py ./
# Seed input baked in; output goes to the mounted volume
COPY exchanges.txt ./
VOLUME ["/app/out"]

# Non-root execution
RUN useradd --create-home runner && chown -R runner /app
USER runner

CMD ["python", "extract_contacts.py"]
```

```bash
# Build
docker build -t sellers-json-extractor .

# Run, persisting contacts.txt on the host (pre-seed it to keep dedupe state)
touch contacts.txt
docker run --rm \
  -v "$(pwd)/exchanges.txt:/app/exchanges.txt:ro" \
  -v "$(pwd)/contacts.txt:/app/contacts.txt" \
  -e HTTPS_PROXY \
  sellers-json-extractor
```

> [!IMPORTANT]
> The script writes `contacts.txt` relative to its working directory (`/app` in the image). Mount the file at `/app/contacts.txt` (as above) so output and dedupe state survive container teardown. To start a fresh harvest, mount a new/empty file.

**Docker Compose equivalent:**

```yaml
services:
  extractor:
    build: .
    volumes:
      - ./exchanges.txt:/app/exchanges.txt:ro
      - ./contacts.txt:/app/contacts.txt      # append-mode output + dedupe state
    environment:
      - HTTPS_PROXY=${HTTPS_PROXY:-}          # pass-through corporate proxy
      - NO_PROXY=${NO_PROXY:-}
    restart: "no"                             # batch job, not a daemon
```

</details>

<details>
<summary><b>⏰ Scheduling — cron & systemd timers</b> (click to expand)</summary>

**cron (nightly at 03:15):**

```cron
15 3 * * * cd /opt/async-sellers-json-contact-extractor && /opt/venv/bin/python extract_contacts.py >> /var/log/sellers-json-extractor.log 2>&1
```

**systemd service + timer:**

```ini
# /etc/systemd/system/sellers-json-extractor.service
[Unit]
Description=Async sellers.json contact extraction
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
WorkingDirectory=/opt/async-sellers-json-contact-extractor
ExecStart=/opt/venv/bin/python extract_contacts.py
User=adops
LimitNOFILE=4096
```

```ini
# /etc/systemd/system/sellers-json-extractor.timer
[Unit]
Description=Weekly sellers.json harvest

[Timer]
OnCalendar=Mon *-*-* 03:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now sellers-json-extractor.timer
```

</details>

<details>
<summary><b>⚙️ CI/CD — GitHub Actions workflow</b> (click to expand)</summary>

```yaml
# .github/workflows/harvest.yml
name: contact-harvest

on:
  schedule:
    - cron: "0 3 * * *"      # 03:00 UTC nightly
  workflow_dispatch:          # manual trigger
  push:
    branches: [main]
    paths:
      - "extract_contacts.py"

jobs:
  verify:                     # quality gate — see the Testing section
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install aiohttp tqdm ruff mypy
      - run: python -m py_compile extract_contacts.py
      - run: ruff check extract_contacts.py

  harvest:
    needs: verify
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install aiohttp tqdm
      - run: python extract_contacts.py
      - uses: actions/upload-artifact@v4
        with:
          name: contacts
          path: contacts.txt
```

> [!CAUTION]
> Scheduled CI runners share egress infrastructure with other users. If you harvest at scale on a schedule, throttle `CONCURRENCY`, cache `contacts.txt` between runs (e.g., `actions/cache` or a committed artifact), and ensure your usage complies with the terms of the sites you fetch.

</details>

---

## Usage

### Basic Usage

The entire workflow is two files and one command: put URLs in `exchanges.txt`, run the script, read emails out of `contacts.txt`.

```bash
# 1. Curate your endpoint list — one URL per line, '#' comments allowed
cat >> exchanges.txt <<'EOF'
https://adingo.jp/sellers.json
https://adform.com/sellers.json
https://amxrtb.com/sellers.json
EOF

# 2. Run the extractor (progress bar renders on stderr)
python extract_contacts.py

# 3. Inspect the harvest
wc -l contacts.txt          # unique email count
head contacts.txt           # one lowercase address per line
```

**Typical console output:**

```text
[i] 1,308 URLs queued | 0 emails already on disk
100%|████████████████████| 1308/1308 [02:11<00:00, 9.92url/s]
[✓] Done. 627 new unique emails written to contacts.txt
[✓] Total unique emails on disk: 627
```

Because the script is also an importable module, you can drive the same primitives programmatically:

```python
import asyncio
from pathlib import Path

import aiohttp

import extract_contacts as extractor  # the CLI file doubles as a library

async def harvest_sample(urls: list[str]) -> list[str]:
    """Fetch a handful of endpoints concurrently and collect valid emails."""
    timeout = aiohttp.ClientTimeout(total=extractor.TIMEOUT_SECONDS)
    async with aiohttp.ClientSession(
        timeout=timeout,
        headers={"User-Agent": extractor.USER_AGENT},  # polite, identifiable UA
        trust_env=True,                                # honor HTTP(S)_PROXY
    ) as session:
        results = await asyncio.gather(
            *(extractor.fetch_email(session, url) for url in urls)
        )
    # fetch_email() returns None for every failure mode — filter those out
    return [email for email in results if email]

# load_urls() skips blank lines and '#' comments for us
urls = extractor.load_urls(Path("exchanges.txt"))
emails = asyncio.run(harvest_sample(urls[:25]))
print(f"sampled {len(emails)} emails, e.g. {emails[:3]}")
```

### Advanced Usage

<details>
<summary><b>⚙️ Tuning throughput & timeouts</b> (click to expand)</summary>

All knobs are module-level constants at the top of `extract_contacts.py`. Edit them in place (or monkeypatch before calling `main()` when importing):

```python
import extract_contacts as extractor

extractor.CONCURRENCY = 50        # gentler on egress / rate-limited IPs
extractor.DNS_THREADS = 150       # keep >= 2-3x CONCURRENCY
extractor.TIMEOUT_SECONDS = 20    # very slow endpoints
extractor.MAX_BYTES = 10 * 1024 * 1024  # 10 MiB for huge sellers.json files

# Then either run the CLI, or drive it yourself:
#   import asyncio; asyncio.run(extractor.main())
```

Rule of thumb: `DNS_THREADS` must stay **well above** `CONCURRENCY`, or dead-domain `getaddrinfo()` calls will occupy the whole resolver pool and stall live fetches.

</details>

<details>
<summary><b>✍️ Custom output writers (CSV / database sink)</b> (click to expand)</summary>

The default `writer_task()` streams newline-delimited emails. For a different sink, reuse the fetch layer and swap the writer — for example, CSV with provenance (requires tracking which URL produced which email; `worker()` already has both in scope):

```python
import asyncio, csv
from pathlib import Path
import aiohttp
import extract_contacts as ex

async def harvest_to_csv(url_file: str, out_file: str) -> None:
    urls = ex.load_urls(Path(url_file))
    timeout = aiohttp.ClientTimeout(total=ex.TIMEOUT_SECONDS)
    rows, seen = [], set()

    async with aiohttp.ClientSession(
        timeout=timeout,
        headers={"User-Agent": ex.USER_AGENT},
        trust_env=True,
    ) as session:
        for batch_start in range(0, len(urls), ex.CONCURRENCY):      # bounded batches
            batch = urls[batch_start : batch_start + ex.CONCURRENCY]
            results = await asyncio.gather(
                *(ex.fetch_email(session, u) for u in batch)
            )
            for url, email in zip(batch, results):
                if email and email not in seen:
                    seen.add(email)
                    rows.append((email, url))                        # provenance!

    with open(out_file, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["email", "source_url"])
        writer.writerows(rows)

asyncio.run(harvest_to_csv("exchanges.txt", "contacts.csv"))
```

Any database sink follows the same pattern: `fetch_email()` is pure fetch-and-extract; persistence is your writer's job.

</details>

<details>
<summary><b>🔌 Running behind a corporate proxy</b> (click to expand)</summary>

No code changes needed — the session is constructed with `trust_env=True`:

```bash
export HTTPS_PROXY=http://user:pass@proxy.internal:3128
export HTTP_PROXY=http://user:pass@proxy.internal:3128
export NO_PROXY=localhost,127.0.0.1,.internal
python extract_contacts.py
```

For proxy auth that must not touch the shell history, use a `.netrc` file or a secrets manager that populates the environment at runtime.

</details>

### Edge Cases & Deep Dives

<details>
<summary><b>🧯 Edge cases the pipeline handles for you</b> (click to expand)</summary>

| Edge case | Behavior | Mechanism |
|---|---|---|
| **Truncated JSON** (response cut at `MAX_BYTES`) | `contact_email` still recovered | `_salvage_contact_email()` scans for the `"contact_email"` key and extracts the next quoted value |
| **Non-JSON body** (HTML error page, plain text) | Silently skipped | `json.JSONDecodeError` → salvage finds nothing → `None` |
| **Non-string `contact_email`** (number, object, array) | Silently skipped | `sanitize_email()` rejects non-`str` values |
| **`mailto:` prefixed address** | Prefix stripped | `sanitize_email()` removes a leading `mailto:` |
| **Mixed case / padded address** | Normalized to lowercase, trimmed | `str.strip().lower()` |
| **Stray punctuation** (`<ops@x.com>`, trailing `;`) | Cleaned | `strip("<>\"' \t\r\n,;")` |
| **Garbage containing `@`** | Rejected | `EMAIL_RE` + 5–254 length bound |
| **UTF-8 BOM** before `{` | Handled | BOM stripped before `json.loads()` |
| **Invalid UTF-8 byte sequences** | Handled | `decode(errors="replace")` — never raises |
| **Non-200 responses (404, 500, 403)** | Silently skipped | status check in `fetch_email()` |
| **Redirect chains** (CDN, vanity hosts) | Followed | `allow_redirects=True` |
| **Dead/slow domains & DNS black holes** | Time out, run continues | 10 s `ClientTimeout` + 300-thread DNS pool |
| **Duplicate emails across many endpoints** | Written once | in-memory `seen` set + pre-load from output file |
| **Rerun after completion** | Zero new writes, exit 0 | `load_existing()` seeds the dedupe set |
| **Ctrl-C mid-run** | Clean message; completed writes kept | `KeyboardInterrupt` handler + per-line `flush()` |
| **`exchanges.txt` missing** | `[!] Input file not found` → exit 1 | explicit check in `main()` |
| **Empty/comment-only `exchanges.txt`** | `[!] No URLs to process.` → exit 0 | `load_urls()` filter |
| **Root JSON is not an object** (array, scalar) | Silently skipped | `isinstance(data, dict)` guard |
| **SSL/TLS errors, connection resets, `OSError`** | Silently skipped per URL | broad-but-typed exception tuple in `fetch_email()` |

</details>

<details>
<summary><b>🧠 Deep dive: the salvage pass</b> (click to expand)</summary>

Large `sellers.json` documents put the `contact_email` key at the **root** — usually within the first few hundred bytes — followed by a `sellers` array that can stretch past the 5 MiB cap. When the cap truncates the body, `json.loads()` fails on valid-but-incomplete input, and a naive approach would lose the email even though it is sitting right there in the buffer.

`_salvage_contact_email()` therefore performs a second, forgiving pass:

1. Find the literal `"contact_email"` in the raw text.
2. Find the first `:` after it.
3. Extract the double-quote-delimited value that follows.
4. Run it through the exact same `sanitize_email()` gate as the strict path.

This is intentionally *not* a general-purpose JSON repair — it is a surgical extraction for the one field we care about, validated afterwards like any other candidate.

</details>

<details>
<summary><b>🎚️ Deep dive: shutdown & queue semantics</b> (click to expand)</summary>

The pipeline uses two queues and one sentinel value (`QUEUE_SENTINEL = None`):

1. `main()` enqueues every URL onto `queue`, then appends exactly `CONCURRENCY` sentinels — one per worker.
2. Each `worker()` loops `queue.get()`; on a sentinel it returns, otherwise it fetches and forwards valid emails to `writer_queue`.
3. `main()` `gather()`s all workers, `join()`s `writer_queue`, then sends the writer its sentinel.
4. `writer_task()` returns the count of *new* emails written.

Every `get()` is paired with `task_done()` in a `finally:` block, so `join()` cannot hang even when a fetch throws. The writer opens the output in **append mode with `buffering=1`** (line-buffered text I/O) and `flush()`es after each write — the durability guarantee behind "Ctrl-C never loses completed work."

</details>

---

## Configuration

> [!NOTE]
> The extractor is deliberately **flag-free and config-file-free**: all tunables are named module constants at the top of `extract_contacts.py`, visible in one screen. The only external configuration surfaces are the two data files (`exchanges.txt`, `contacts.txt`) and standard proxy environment variables.

### Module Constants

<details>
<summary><b>📜 Exhaustive constants reference</b> (click to expand)</summary>

| Constant | Default | Type | Description | Effect of changing |
|---|---|---|---|---|
| `INPUT_FILE` | `Path("exchanges.txt")` | `pathlib.Path` | Seed list of `sellers.json` URLs, one per line; `#` comments and blank lines ignored | Point at any other URL list (cwd-relative or absolute) |
| `OUTPUT_FILE` | `Path("contacts.txt")` | `pathlib.Path` | Destination for unique emails, one per line; opened in **append** mode | Any writable path; remember it also seeds the dedupe set |
| `CONCURRENCY` | `100` | `int` | Number of concurrent worker coroutines (and the session connection `limit`) | Higher = faster, more sockets/egress pressure; lower for rate-limited environments |
| `DNS_THREADS` | `300` | `int` | Max workers in the `getaddrinfo()` thread pool | Must stay **≫ `CONCURRENCY`**; shrinking it toward 100 risks DNS starvation |
| `MAX_BYTES` | `5 * 1024 * 1024` (5 MiB) | `int` | Hard cap on bytes downloaded per URL (chunked read stops here) | Raise for huge `sellers.json`; the salvage pass mitigates truncation either way |
| `TIMEOUT_SECONDS` | `10` | `int` | `aiohttp.ClientTimeout` for both `total` and `connect` | Raise on very slow endpoints; lower for aggressive fail-fast sweeps |
| `QUEUE_SENTINEL` | `None` | `None` | Poison-pill marker for worker/writer queue shutdown | Rarely changed; must stay a value that can never be a real work item |
| `USER_AGENT` | `Mozilla/5.0 (compatible; SellersJsonExtractor/1.0; +https://iabtechlab.com/sellers.json/)` | `str` | Sent on every request; identifies the client politely | Customize to your org's contact URL if you need to be identifiable |
| `EMAIL_RE` | `^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$` | `re.Pattern` | Conservative email-shape validator applied after sanitization | Loosen only if you accept riskier addresses |
| *(connector)* `limit_per_host` | `8` | `int` (in `TCPConnector`) | Max simultaneous connections per host | Raise only against infrastructure you control |
| *(connector)* `ssl` | `False` | `bool` (in `TCPConnector`) | TLS certificate verification disabled | Set to `True` for strict environments — some harvestable hosts will fail |
| *(connector)* `ttl_dns_cache` | `300` | `int` (in `TCPConnector`) | DNS cache TTL in seconds | Raise for stable corpora; lower if DNS records churn |
| *(connector)* `enable_cleanup_closed` | `True` | `bool` (in `TCPConnector`) | Cleans up broken keep-alive transports | Keep `True` on long runs |
| *(session)* `trust_env` | `True` | `bool` (in `ClientSession`) | Honors `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY` | Set `False` to ignore ambient proxy settings |
| *(session)* `allow_redirects` | `True` | `bool` (in `session.get`) | Follow 3xx chains | Set `False` in `fetch_email()` if you need first-party hosts only |
| *(stream)* chunk size | `64 * 1024` (64 KiB) | `int` (in `iter_chunked`) | Download granularity used to enforce `MAX_BYTES` | Smaller = finer-grained cap enforcement, slightly slower |
| *(writer)* open mode | `"a"` + `buffering=1` | — | Append mode, line-buffered, `flush()` per write | Change to `"w"` for a fresh, overwrite-each-run workflow |

</details>

### Input & Output Formats

<details>
<summary><b>📄 File schemas — `exchanges.txt` & `contacts.txt`</b> (click to expand)</summary>

**`exchanges.txt` (input)** — UTF-8 text, parsed by `load_urls()`:

```text
# Comments start with '#' and are ignored
# Blank lines are ignored

https://adingo.jp/sellers.json          # http:// and https:// both accepted
https://tag.adbro.me/rtb/sellers.json   # subpaths fine
http://sellers.adipolosolutions.com/631b56632cc1123bd77adfe2/sellers.json
```

* One URL per line; lines are `strip()`ed before parsing.
* Lines that are empty after stripping, or that begin with `#`, are skipped.
* No URL validation happens at load time — malformed URLs simply fail at fetch and are skipped like any other error.
* The repository ships a curated snapshot of **~1,308** ad-exchange/SSP endpoints as a ready-to-run starting point.

**`contacts.txt` (output)** — UTF-8 text, written by `writer_task()`:

```text
amir@adipolo.com
adserver@agora.pl
contact@gemius.ro
sellers-json@microsoft.com
```

* One **lowercased, sanitized** email per line, in completion order.
* Globally unique within the file (guaranteed by the `seen` set, including addresses pre-loaded from previous runs).
* **Append mode:** reruns extend the file; delete it (or point `OUTPUT_FILE` elsewhere) to start a clean harvest.
* The shipped snapshot contains **~627** unique contacts and doubles as the dedupe seed for your next run.

</details>

### Environment Variables

<details>
<summary><b>🌍 Environment variables honored by the runtime</b> (click to expand)</summary>

The tool has **no variables of its own** (no `.env` loader, no `getenv` calls). It inherits the standard environment understood by `aiohttp` (`trust_env=True`), `ssl`, and Python itself:

| Variable | Consumed by | Effect |
|---|---|---|
| `HTTP_PROXY` / `http_proxy` | `aiohttp` (`trust_env=True`) | Routes plain-HTTP requests through the given proxy |
| `HTTPS_PROXY` / `https_proxy` | `aiohttp` (`trust_env=True`) | Routes HTTPS requests through the given proxy |
| `NO_PROXY` / `no_proxy` | `aiohttp` (`trust_env=True`) | Comma-separated host/domain bypass list |
| `SSL_CERT_FILE`, `SSL_CERT_DIR` | `ssl` module | Custom CA bundles (relevant if you enable certificate verification) |
| `PYTHONUNBUFFERED` | CPython | Unbuffered stdio — handy under cron/systemd so log lines appear immediately |
| `COLUMNS` | `tqdm` | Overrides terminal width for the progress bar in non-TTY environments |

**Startup flags:** none — the CLI takes no arguments by design. Invocation is always simply:

```bash
python extract_contacts.py
```

> [!WARNING]
> `TCPConnector(ssl=False)` disables TLS certificate verification to maximize harvest yield across misconfigured ad-tech hosts. This makes the client theoretically vulnerable to man-in-the-middle interception on untrusted networks. For strict environments, set `ssl=True` in the connector call (and expect some endpoints with broken/expired certificates to be skipped).

> [!CAUTION]
> The default profile fires up to **100 concurrent requests** with a per-host cap of 8. That is well-behaved for a one-shot sweep of thousands of *distinct* hosts, but do not crank `CONCURRENCY` and `limit_per_host` up and aim it at a single organization's infrastructure. Stay within the target sites' terms of service and applicable law.

</details>

---

## License

Licensed under the **Apache License, Version 2.0** (see the [`LICENSE`](LICENSE) file for the complete legal text).

You are free to:

- **Use** the software commercially and privately.
- **Modify** it and create derivative works.
- **Distribute** copies of the software and derivatives.
- **Sublicense** and provide warranties in your own distributions.

Under the following conditions:

- **Attribution** — retain copyright and license notices; state significant changes in modified files; keep the `NOTICE`-style attribution intact in derivative distributions.

The software is provided **"AS IS"**, without warranty of any kind; the authors are not liable for damages arising from its use. Refer to the full [Apache-2.0 text](http://www.apache.org/licenses/LICENSE-2.0) for the authoritative terms.

---

## Contacts & Community Support

## Support the Project

[![Patreon](https://img.shields.io/badge/Patreon-OstinFCT-f96854?style=flat-square&logo=patreon)](https://www.patreon.com/OstinFCT)
[![Ko-fi](https://img.shields.io/badge/Ko--fi-fctostin-29abe0?style=flat-square&logo=ko-fi)](https://ko-fi.com/fctostin)
[![Boosty](https://img.shields.io/badge/Boosty-Support-f15f2c?style=flat-square)](https://boosty.to/ostinfct)
[![YouTube](https://img.shields.io/badge/YouTube-FCT--Ostin-red?style=flat-square&logo=youtube)](https://www.youtube.com/@FCT-Ostin)
[![Telegram](https://img.shields.io/badge/Telegram-FCTostin-2ca5e0?style=flat-square&logo=telegram)](https://t.me/FCTostin)

If you find this tool useful, consider leaving a star on GitHub or supporting the author directly.
