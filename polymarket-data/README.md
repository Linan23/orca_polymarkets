# Polymarket Data

## Event By ID Scraper

`event_by_id_scraper.py` scrapes the Polymarket Gamma API event-by-id endpoint and appends each response as JSONL.

### Run every hour (default)

```bash
cd <repo-root>
.venv/bin/python polymarket-data/event_by_id_scraper.py --event-id 162522
```

### Customize interval

Every 30 minutes:

```bash
.venv/bin/python polymarket-data/event_by_id_scraper.py --event-id 162522 --interval-minutes 30
```

Every 2 hours:

```bash
.venv/bin/python polymarket-data/event_by_id_scraper.py --event-id 162522 --interval-hours 2
```

Run only during a daily time window:

```bash
.venv/bin/python polymarket-data/event_by_id_scraper.py \
  --event-id 162522 \
  --interval-minutes 15 \
  --window-start 09:00 \
  --window-end 17:00 \
  --timezone America/New_York
```

Quick one-shot test:

```bash
.venv/bin/python polymarket-data/event_by_id_scraper.py --event-id 162522 --max-requests 1 --interval-seconds 1
```

### Output

By default, results are written to:

`polymarket-data/event_by_id.jsonl`

Each line contains:

- scrape timestamp metadata
- event id and summary fields
- full API payload under `data`

## Dynamic Event Discovery Scraper

`discover_events_scraper.py` finds events from the `/events` list endpoint, so you do not need fixed event ids.

### Discover open active events every hour (default)

```bash
cd <repo-root>
.venv/bin/python polymarket-data/discover_events_scraper.py
```

### Discover only BTC-related events and fetch full details

```bash
.venv/bin/python polymarket-data/discover_events_scraper.py \
  --query-text btc \
  --fetch-full-details \
  --per-event-delay-seconds 2
```

### Discover by tag instead of specific ids

This discovers events tagged `crypto` and fetches full details for all matches:

```bash
.venv/bin/python polymarket-data/discover_events_scraper.py \
  --tag crypto \
  --fetch-full-details \
  --per-event-delay-seconds 2
```

This requires all listed tags to be present on each event:

```bash
.venv/bin/python polymarket-data/discover_events_scraper.py \
  --active any \
  --closed any \
  --tag bitcoin \
  --tag crypto \
  --tag-mode all \
  --fetch-full-details
```

### Change the interval

```bash
.venv/bin/python polymarket-data/discover_events_scraper.py --interval-minutes 15
```

### Run only during a daily time window

This runs every 10 minutes, but only between 9:00 AM and 5:00 PM New York time:

```bash
.venv/bin/python polymarket-data/discover_events_scraper.py \
  --interval-minutes 10 \
  --window-start 09:00 \
  --window-end 17:00 \
  --timezone America/New_York
```

### Discover events dynamically, fetch full details, and pace requests

This discovers matching events, fetches each event-by-id payload, waits between event requests, and repeats on a schedule:

```bash
.venv/bin/python polymarket-data/discover_events_scraper.py \
  --active any \
  --closed any \
  --limit 100 \
  --query-text bitcoin \
  --fetch-full-details \
  --per-event-delay-seconds 2 \
  --interval-minutes 15 \
  --window-start 09:00 \
  --window-end 17:00 \
  --timezone America/New_York
```

### Useful flags

- `--active true|false|any`
- `--closed true|false|any`
- `--limit`
- `--query-text` (matches against event title + slug)
- `--tag` (matches against tag labels and tag slugs)
- `--tag-mode any|all`
- `--fetch-full-details`
- `--per-event-delay-seconds`
- `--window-start`, `--window-end` (daily HH:MM window)
- `--timezone`
- `--interval-hours`, `--interval-minutes`, `--interval-seconds`
- `--write-to-db` (persist normalized rows into PostgreSQL as well as JSONL)
- `--database-url` (optional override for `DATABASE_URL`)

### Write directly into PostgreSQL

```bash
.venv/bin/python polymarket-data/discover_events_scraper.py \
  --tag crypto \
  --fetch-full-details \
  --write-to-db \
  --max-requests 1
```

## Batch Event Scraper

`batch_event_scraper.py` fetches multiple Polymarket events by id in one cycle and writes one JSONL record per cycle.

### Scrape multiple event ids every hour (default)

```bash
cd <repo-root>
.venv/bin/python polymarket-data/batch_event_scraper.py \
  --event-id 162522 \
  --event-id 162489
```

### Use a file of event ids

```bash
.venv/bin/python polymarket-data/batch_event_scraper.py \
  --event-ids-file polymarket-data/event_ids.txt
```

### Add pacing and a daily active window

```bash
.venv/bin/python polymarket-data/batch_event_scraper.py \
  --event-id 162522 \
  --event-id 162489 \
  --per-event-delay-seconds 2 \
  --interval-minutes 15 \
  --window-start 09:00 \
  --window-end 17:00 \
  --timezone America/New_York
```

### Partial failure behavior

If one event id fails (for example a `404`), the batch still completes and records that failure in the `errors` array while saving successful events in `results`.
