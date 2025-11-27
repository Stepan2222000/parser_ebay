# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Говори со мной только по русски

## Overview

eBay product scraper and validator that extracts listings from eBay based on part numbers/queries, applies price filters and blacklists, stores results in PostgreSQL, and publishes to Redis Streams for downstream validation services. Built with Python 3.12, uses async/await throughout with Taskiq for distributed task processing.

## Architecture

### Core Components

**Task Distribution (Taskiq + Redis)**
- Two worker queues: `broker_cl` (catalog worker, 20 workers) and `broker_pw` (Playwright/browser worker, 170 workers)
- Queue names: `cl_my` and `pw_my` in Redis
- `main.py` orchestrates: catalog scanning → filtering → individual product parsing → DB storage → Redis Stream publishing

**Data Flow**
1. **Input**: Excel file with part numbers (column A) and optional price limits (column B), or DB-driven mode
2. **Catalog Phase** (`task_collect_loop`): Fetch eBay catalog pages for each query
3. **Filtering**: Price threshold, seller blacklist, title blocklist, duplicate detection
4. **Product Phase** (`product`): Parse individual listings for details + item_specifics
5. **Storage**: Batch commit to PostgreSQL (`PackageCommit`)
6. **Publishing**: Push to Redis Stream for external validation service

**Proxy & Session Management**
- Rotating proxies from `proxies.txt` (format: `host:port:user:pass`)
- Two-tier fetching: aiohttp (fast, primary) → Playwright (fallback for bot detection)
- Cookie warming via Playwright to bypass eBay challenges
- Sessions stored in Redis db=4

### Database Schema

**Primary Tables** (PostgreSQL via asyncpg)
- `ebay`: Main listings table (number, title, price, location, seller, query, condition, Brand)
- `item_specifics`: Key-value pairs extracted from listing details (foreign key to `ebay.number`)

**Batch Writing**: `PackageCommit` class accumulates inserts, commits in configurable batches (`DB_BATCH_SIZE`, `DB_BATCH_DELAY`)

### Optional Features (Environment Toggle)

**SMART Price Provider** (`USE_SMART_PRICE=1`)
- Replaces Excel price limits with database lookup from `parts_admin.public.smart` + `ebay_admin.public.parts_prices`
- Article normalization: uppercase + strip non-alphanumeric for matching
- Falls back to Excel column B if DB lookup fails

**Title Recheck** (`RECHECK_TITLES=1`)
- Detects catalog title changes vs. stored DB titles
- Deletes stale records and triggers re-parsing
- Logs mismatches to `title_mismatch.log` (JSONL)

**Duplicate Cache** (`DUPLICATE_CACHE_ENABLED=1`)
- Filesystem-based deduplication guard using `duplicate_cache.txt`
- Prevents re-parsing recently seen item IDs

**Blocked Title Filter** (`BLOCKED_TITLE_FILTER=1`)
- Rejects listings containing keywords from `BLOCKED_TITLE_WORDS` (comma-separated)
- Optional whitelist mode (`BLOCKED_TITLE_WHITELIST_FILTER=1`)

## Common Commands

### Running the Application

**Excel Mode** (reads queries from Excel, one-shot or continuous)
```bash
# Start all services (redis, nats, workers, main producer)
docker compose up -d

# View logs for specific service
docker compose logs -f app-main
docker compose logs -f app-worker-cl
docker compose logs -f app-worker-pw

# Stop all services
docker compose down
```

**Main service** (`app-main`): Runs `main_excel.py`, reads `EXCEL_PATH` and enqueues queries to `cl_my`

**Worker services**:
- `app-worker-cl`: Catalog parsing (aiohttp-based)
- `app-worker-pw`: Browser automation (Playwright Firefox)

### Development & Testing

**Build Docker image**
```bash
docker compose build
```

**Run Redis bootstrap** (initializes Redis state)
```bash
docker compose run --rm redis-bootstrap
```

**Run Excel mode locally** (outside Docker, requires venv)
```bash
python3 -m main_excel
# or
python3 main_excel.py
```

**Check worker health** (via heartbeat)
- Workers write heartbeat keys `hb:<WORKER_ID>` to Redis db=2 with TTL
- Check logs in `heartbeat.log`

### Debugging

**Enable verbose logging**
```bash
# In .env or docker-compose.yml environment
EXCEL_VERBOSE=1              # Detailed catalog filtering decisions
DEBUG_REQUESTS=1             # Log all HTTP request statuses
RECHECK_TITLES_LOG=1         # Title change events
BLOCKED_TITLE_LOG=1          # Rejected listings by title filter
```

**Log files** (written to project root)
- `tracking_debug.log`: Core lifecycle events (product start/commit, batch sizes)
- `excel_verbose.log`: Per-item catalog decisions (price filtering, accept/reject)
- `requests_aiohttp.log` / `requests_playwright.log`: HTTP status codes
- `title_mismatch.log`: Title recheck mismatches (JSONL)
- `blocked_title.log`: Title filter rejections
- `duplicate_cache.log`: Duplicate detection events
- `heartbeat.log`: Worker liveness signals
- `asyncio_guard.log`: Suppressed asyncio race conditions

**Manual Redis inspection**
```bash
# Connect to internal Redis (Docker network)
docker compose exec redis redis-cli

# Check queue lengths
LLEN cl_my
LLEN pw_my

# Check processing set
ZCARD cl_my:processing
ZRANGE cl_my:processing 0 -1 WITHSCORES

# Check Redis Stream
XLEN ebay_validation_stream
XREAD COUNT 5 STREAMS ebay_validation_stream 0
```

**Batch runner** (process multiple Excel files sequentially)
```bash
python3 batch_runner.py
```
Reads all `.xlsx` files from `files/` directory, updates `.env` with `EXCEL_PATH`, restarts `app-main` service, waits for completion.

## Key Environment Variables

**Redis & Streams**
- `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`: Connection to external Redis
- `REDIS_STREAM_KEY`: Stream name for publishing (default: `ebay_validation_stream`)

**Excel Mode**
- `EXCEL_PATH`: Path to Excel file (default: `/opt/app/iee.xlsx`)
- `EXCEL_VERBOSE`: Enable detailed filtering logs
- `CATALOG_ONE_SHOT`: Producer exits after enqueuing all queries once (default: 1 in Docker)

**Database Connections**
- `RECHECK_DB_*`: Postgres connection for title recheck (host, port, db, user, password)
- `PARTS_ADMIN_*` / `EBAY_ADMIN_*`: Connections for SMART price provider

**Performance Tuning**
- `DB_BATCH_SIZE`: Number of items per batch commit (default: 1)
- `DB_BATCH_DELAY`: Seconds between batch flushes (default: 0.05)
- `PACKAGE_COMMIT_TIMEOUT`: Max seconds for commit operation (default: 300)
- `CATALOG_PROCESSING_STALE`: Seconds before re-queuing stale tasks (default: 60)

**Feature Flags**
- `USE_SMART_PRICE`: Enable database-driven price limits (0/1)
- `RECHECK_TITLES`: Enable title change detection (0/1)
- `DUPLICATE_CACHE_ENABLED`: Enable filesystem deduplication (0/1)
- `BLOCKED_TITLE_FILTER`: Enable title keyword blocking (0/1)
- `HEARTBEAT_ENABLED`: Enable worker heartbeat monitoring (0/1)

## Development Notes

**Agent Rules** (see [AGENTS.md](AGENTS.md))
- Always communicate in Russian per project conventions
- Use `mcp context7` for library documentation lookups
- Terminal-first approach: run many verification commands
- KISS principle: keep solutions simple and transparent
- Never claim work is done without proving it via logs/commands

**Testing Pattern**
1. Check venv exists and is activated
2. Install/update dependencies from `pyproject.toml` via `uv`
3. Run linters/formatters if configured
4. Execute commands and collect proof (logs, output)
5. Verify no regressions in adjacent modules

**Container-based Development**
- Dockerfile uses Python 3.12.9 compiled from source with optimizations
- `uv` package manager for fast dependency installation
- Playwright Firefox with `undetected-playwright` to avoid bot detection
- Services depend on `redis-bootstrap` completing successfully

**Proxy Management**
- Format: one proxy per line in `proxies.txt`
- Proxies cycle through requests
- Healthcheck via `PROXY_HEALTHCHECK_URL` before use
- Session cookies cached per proxy in Redis db=4

**Duplicate Prevention**
- Redis-based guard in db=5 for cross-worker coordination
- Optional filesystem cache for persistence across restarts
- Catalog-level deduplication via `cl_my:dedupe_queries` Redis set

**Error Handling**
- Retry decorator with configurable attempts and delays
- Playwright fallback when aiohttp blocked by eBay
- Graceful degradation: Redis Stream publish failures log warnings but don't block main flow
- `TypeErrorExitHandler` suppresses known asyncio race conditions in Python 3.12

## Project Structure

- `main.py`: Core scraping logic (catalog/product parsing, filtering, DB writes)
- `main_excel.py`: Entry point for Excel-driven mode
- `batch_runner.py`: Multi-file Excel processing orchestrator
- `package_commit.py`: Batched PostgreSQL insert manager
- `redis_stream_producer.py`: Redis Stream publishing
- `excel_source.py`: Excel file parser (columns A/B)
- `excel_log.py`: Verbose filtering event logger
- `request_log.py`: HTTP request status tracker
- `duplicate_guard.py` / `duplicate_cache.py`: Deduplication systems
- `title_recheck.py`: Title change detection module
- `price_provider_smart.py`: Database-driven price lookup
- `blocked_title_filter.py`: Title keyword filtering
- `heartbeat.py`: Worker liveness monitoring
- `catalog_less_match_guard.py`: Catalog completion detection
- `xpath_debug.py` / `html_text_logger.py`: Debugging utilities
- `bootstrap_internal_redis.py`: Initial Redis state setup
- `proxies.txt`: Proxy list (not version controlled)
- `.env`: Environment configuration (contains credentials, not version controlled)
- `docker-compose.yml`: Multi-container orchestration
- `Dockerfile`: Python 3.12 + Playwright + dependencies
