# kalshi-starter-code-python
Example python code for accessing api-authenticated endpoints on [Kalshi](https://kalshi.com). This is not an SDK. 

## Installation 
Install requirements.txt in a virtual environment of your choice and execute main.py from within the repo.

```
pip install -r requirements.txt
python main.py
```

## Environment variables

Set these in a `.env` file in this folder:

```
DEMO_KEYID=your_demo_api_key_id
DEMO_KEYFILE=/absolute/path/to/demo_private_key.pem
PROD_KEYID=your_prod_api_key_id
PROD_KEYFILE=/absolute/path/to/prod_private_key.pem
```

## Formal code documentation

Function-level API documentation (description, parameters, outputs, and error behavior) is available in:

- `API_REFERENCE.md`

## Scheduled scraper

`main.py` supports timed polling for authenticated endpoints with retry, `Retry-After` handling on `429`, jitter, and JSONL output.

### Example: scrape exchange status every 5 seconds

```
python main.py --environment demo --endpoint status --interval-seconds 5 --jitter-seconds 1
```

### Example: scrape trades to JSONL file

```
python main.py \
	--environment demo \
	--endpoint trades \
	--ticker KXBTCD-26FEB26-B100000 \
	--limit 200 \
	--interval-seconds 10 \
	--jitter-seconds 1 \
	--max-retries 6 \
	--output-file kalshi_trades.jsonl
```

### Example: scrape any authenticated GET endpoint path

```
python main.py \
	--environment demo \
	--endpoint custom \
	--path /trade-api/v2/portfolio/orders \
	--query-param status=open \
	--query-param limit=100 \
	--interval-seconds 8 \
	--jitter-seconds 1.5 \
	--timeout-seconds 20 \
	--output-file kalshi_orders.jsonl
```

### Example: run only during a daily time window

```
python main.py \
	--environment prod \
	--endpoint status \
	--interval-seconds 600 \
	--window-start 09:00 \
	--window-end 17:00 \
	--timezone America/New_York \
	--output-file kalshi_status_windowed.jsonl
```

### Example: write trades to PostgreSQL and JSONL

```
python main.py \
	--environment prod \
	--endpoint trades \
	--limit 100 \
	--write-to-db \
	--output-file kalshi_trades.jsonl \
	--max-requests 1
```

### Useful flags

- `--endpoint`: `balance`, `status`, `trades`, or `custom`
- `--path`: required when `--endpoint custom`
- `--query-param key=value`: repeatable query params for custom endpoints
- `--interval-seconds`: target cycle duration (request time included)
- `--window-start`, `--window-end`: optional daily HH:MM active window
- `--timezone`: timezone used for window checks
- `--jitter-seconds`: randomized extra delay to avoid fixed request cadence
- `--max-requests`: stop after N requests (`0` = run forever)
- `--max-retries`: retries per request when rate-limited or on transient failures
- `--backoff-base-seconds`, `--backoff-cap-seconds`: exponential backoff controls
- `--timeout-seconds`: HTTP timeout per request
- `--output-file`: append JSONL records to file
- `--write-to-db`: persist normalized rows into PostgreSQL as well as JSONL
- `--database-url`: optional override for `DATABASE_URL`
