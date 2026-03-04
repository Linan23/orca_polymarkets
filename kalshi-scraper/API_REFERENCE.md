# Kalshi Scraper API Reference

This document describes the callable interfaces in `main.py` and `clients.py`:
- Purpose of each function or method.
- Parameters and accepted values.
- Return/output shape.
- Error handling behavior.

## main.py

### `parse_query_params(items: list[str]) -> dict[str, str]`
- Description: Parses repeated CLI `--query-param` entries from `key=value` format.
- Parameters:
  - `items`: List of strings, each expected to be `key=value`.
- Returns:
  - Dictionary mapping query parameter keys to values.
- Errors:
  - Raises `ValueError` when a param is malformed or key is empty.

### `parse_args() -> argparse.Namespace`
- Description: Parses and validates all scraper CLI arguments.
- Parameters:
  - None (reads from process CLI).
- Returns:
  - `argparse.Namespace` containing validated options and `custom_query_params`.
- Errors:
  - Raises `SystemExit` via `argparse` for invalid combinations/values.

### `get_credentials(environment: Environment) -> tuple[str, str]`
- Description: Resolves key ID and private key file path from environment variables.
- Parameters:
  - `environment`: `Environment.DEMO` or `Environment.PROD`.
- Returns:
  - Tuple `(key_id, key_file_path)`.
- Errors:
  - Raises `ValueError` if required env vars are missing.

### `load_private_key(key_file: str) -> rsa.RSAPrivateKey`
- Description: Loads PEM private key used for Kalshi request signing.
- Parameters:
  - `key_file`: Path to PEM-encoded RSA private key.
- Returns:
  - `rsa.RSAPrivateKey` object.
- Errors:
  - Raises `FileNotFoundError` if file does not exist.
  - Raises `Exception` when key parsing fails.

### `fetch_once(client: KalshiHttpClient, args: argparse.Namespace) -> dict[str, Any]`
- Description: Dispatches one API request to `balance`, `status`, `trades`, or `custom` endpoint.
- Parameters:
  - `client`: Configured authenticated HTTP client.
  - `args`: Parsed CLI options controlling endpoint and filters.
- Returns:
  - Parsed JSON payload (`dict`) from API response.
- Errors:
  - Propagates `requests.RequestException` subclasses from HTTP layer.

### `parse_retry_after_seconds(retry_after: str) -> float | None`
- Description: Converts `Retry-After` header value to seconds.
- Parameters:
  - `retry_after`: Raw header value (seconds or HTTP date).
- Returns:
  - Delay in seconds as `float`, or `None` if parsing fails.
- Errors:
  - No exception is raised for parse failures; returns `None`.

### `compute_retry_delay(attempt: int, args: argparse.Namespace, retry_after_seconds: float | None = None) -> float`
- Description: Builds retry delay from exponential backoff + jitter, optionally honoring `Retry-After`.
- Parameters:
  - `attempt`: Zero-based retry attempt number.
  - `args`: Parsed CLI options with backoff configuration.
  - `retry_after_seconds`: Optional server-provided delay hint.
- Returns:
  - Delay in seconds before next retry attempt.
- Errors:
  - None under normal usage.

### `fetch_with_backoff(client: KalshiHttpClient, args: argparse.Namespace) -> dict[str, Any]`
- Description: Fetches endpoint data with retries for transient HTTP/network failures.
- Parameters:
  - `client`: Configured authenticated HTTP client.
  - `args`: Parsed CLI options including retry settings.
- Returns:
  - Successful JSON payload.
- Errors:
  - Raises `HTTPError` for non-retryable HTTP errors or after max retries.
  - Raises `requests.RequestException` for network failures after max retries.

### `append_jsonl(output_file: str, record: dict) -> None`
- Description: Appends one serialized scrape record as a JSON line.
- Parameters:
  - `output_file`: JSONL file path; no-op when empty.
  - `record`: JSON-serializable dictionary.
- Returns:
  - `None`.
- Errors:
  - Raises `OSError` for filesystem failures.
  - Raises `TypeError` for non-serializable values.

### `main() -> None`
- Description: Entrypoint that initializes config/auth and runs the polling loop.
- Parameters:
  - None.
- Returns:
  - `None`.
- Errors:
  - Raises credential/key loading errors before loop starts.
  - During loop, propagates request exceptions when retries are exhausted.
  - Handles `KeyboardInterrupt` gracefully and exits.

## clients.py

### `Environment(Enum)`
- Description: Target API environment enum.
- Values:
  - `DEMO = "demo"`
  - `PROD = "prod"`

## `KalshiBaseClient`

### `__init__(key_id: str, private_key: rsa.RSAPrivateKey, environment: Environment = Environment.DEMO)`
- Description: Initializes shared API credentials and environment URLs.
- Parameters:
  - `key_id`: Kalshi API key identifier.
  - `private_key`: RSA private key for request signing.
  - `environment`: Target API environment.
- Returns:
  - None.
- Errors:
  - Raises `ValueError` for unknown environment.

### `request_headers(method: str, path: str) -> Dict[str, Any]`
- Description: Builds signed auth headers for an API request.
- Parameters:
  - `method`: HTTP method string.
  - `path`: Endpoint path; query section is excluded from signing.
- Returns:
  - Header dict including Kalshi auth fields.
- Errors:
  - Raises `ValueError` if signature generation fails.

### `sign_pss_text(text: str) -> str`
- Description: Signs text with RSA-PSS and returns Base64 signature.
- Parameters:
  - `text`: Canonical message to sign.
- Returns:
  - Base64 signature string.
- Errors:
  - Raises `ValueError` on signing failure.

## `KalshiHttpClient(KalshiBaseClient)`

### `__init__(key_id: str, private_key: rsa.RSAPrivateKey, environment: Environment = Environment.DEMO)`
- Description: Configures HTTP base host, API prefixes, and `requests.Session`.
- Parameters:
  - Same credential/environment parameters as base class.
- Returns:
  - None.

### `rate_limit() -> None`
- Description: Enforces a minimum spacing between outgoing API calls.
- Parameters:
  - None.
- Returns:
  - `None`.
- Errors:
  - None.

### `raise_if_bad_response(response: requests.Response) -> None`
- Description: Raises for non-2xx HTTP responses.
- Parameters:
  - `response`: HTTP response object.
- Returns:
  - `None`.
- Errors:
  - Raises `requests.HTTPError` for bad status codes.

### `normalize_path(path: str) -> str`
- Description: Ensures API path has leading slash.
- Parameters:
  - `path`: Input endpoint path.
- Returns:
  - Normalized path string.

### `_request(method: str, path: str, params: Optional[Dict[str, Any]] = None, body: Optional[Dict[str, Any]] = None, timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS) -> Any`
- Description: Low-level authenticated request executor.
- Parameters:
  - `method`: HTTP method.
  - `path`: API path.
  - `params`: Optional query dict.
  - `body`: Optional JSON request body.
  - `timeout_seconds`: Request timeout.
- Returns:
  - Parsed JSON response.
- Errors:
  - Raises `requests.RequestException` for request errors.
  - Raises `requests.HTTPError` for non-2xx responses.

### `post(path: str, body: dict, timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS) -> Any`
- Description: Authenticated `POST`.
- Parameters:
  - `path`, `body`, `timeout_seconds`.
- Returns:
  - Parsed JSON response.
- Errors:
  - Propagates request and HTTP errors from `_request`.

### `get(path: str, params: Optional[Dict[str, Any]] = None, timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS) -> Any`
- Description: Authenticated `GET`.
- Parameters:
  - `path`, `params`, `timeout_seconds`.
- Returns:
  - Parsed JSON response.
- Errors:
  - Propagates request and HTTP errors from `_request`.

### `delete(path: str, params: Optional[Dict[str, Any]] = None, timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS) -> Any`
- Description: Authenticated `DELETE`.
- Parameters:
  - `path`, `params`, `timeout_seconds`.
- Returns:
  - Parsed JSON response.
- Errors:
  - Propagates request and HTTP errors from `_request`.

### `get_balance(timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS) -> Dict[str, Any]`
- Description: Fetches portfolio balance.
- Parameters:
  - `timeout_seconds`.
- Returns:
  - Balance payload dict.

### `get_exchange_status(timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS) -> Dict[str, Any]`
- Description: Fetches exchange status.
- Parameters:
  - `timeout_seconds`.
- Returns:
  - Exchange status payload dict.

### `get_trades(ticker: Optional[str] = None, limit: Optional[int] = None, cursor: Optional[str] = None, max_ts: Optional[int] = None, min_ts: Optional[int] = None, timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS) -> Dict[str, Any]`
- Description: Fetches market trades with optional filters.
- Parameters:
  - `ticker`: Optional market ticker.
  - `limit`: Optional max records.
  - `cursor`: Optional pagination cursor.
  - `max_ts`: Optional upper timestamp bound.
  - `min_ts`: Optional lower timestamp bound.
  - `timeout_seconds`.
- Returns:
  - Trades payload dict.

### `get_path(path: str, params: Optional[Dict[str, Any]] = None, timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS) -> Dict[str, Any]`
- Description: Fetches arbitrary authenticated GET endpoint.
- Parameters:
  - `path`, `params`, `timeout_seconds`.
- Returns:
  - Endpoint payload dict.

## `KalshiWebSocketClient(KalshiBaseClient)`

### `__init__(key_id: str, private_key: rsa.RSAPrivateKey, environment: Environment = Environment.DEMO)`
- Description: Initializes websocket URL state and message counter.
- Parameters:
  - Same credential/environment parameters as base class.
- Returns:
  - None.

### `connect()`
- Description: Opens authenticated websocket and enters message handler.
- Parameters:
  - None.
- Returns:
  - `None` (async).
- Errors:
  - Propagates websocket connection/runtime exceptions.

### `on_open()`
- Description: Connection-open hook; subscribes to ticker channel.
- Parameters:
  - None.
- Returns:
  - `None` (async).

### `subscribe_to_tickers()`
- Description: Sends ticker channel subscription message.
- Parameters:
  - None.
- Returns:
  - `None` (async).
- Errors:
  - Propagates websocket send exceptions.

### `handler()`
- Description: Receives and dispatches incoming websocket messages.
- Parameters:
  - None.
- Returns:
  - `None` (async).
- Errors:
  - Handles connection close by calling `on_close`.
  - Handles other exceptions by calling `on_error`.

### `on_message(message)`
- Description: Per-message callback.
- Parameters:
  - `message`: Raw websocket message.
- Returns:
  - `None` (async).

### `on_error(error)`
- Description: Error callback.
- Parameters:
  - `error`: Exception object.
- Returns:
  - `None` (async).

### `on_close(close_status_code, close_msg)`
- Description: Close-event callback.
- Parameters:
  - `close_status_code`: Websocket close status code.
  - `close_msg`: Close reason text.
- Returns:
  - `None` (async).
