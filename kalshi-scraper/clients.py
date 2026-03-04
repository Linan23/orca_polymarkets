"""Authenticated Kalshi API clients for HTTP and WebSocket access.

This module defines:
1. Shared signing/authentication behavior.
2. HTTP helpers for authenticated REST endpoints.
3. WebSocket helpers for authenticated streaming subscriptions.
"""

import base64
import time
from typing import Any, Dict, Optional
from datetime import datetime, timedelta
from enum import Enum
import json

import requests

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.exceptions import InvalidSignature

import websockets

class Environment(Enum):
    """Supported Kalshi API environments."""
    DEMO = "demo"
    PROD = "prod"

class KalshiBaseClient:
    """Base class with Kalshi authentication and environment configuration."""
    def __init__(
        self,
        key_id: str,
        private_key: rsa.RSAPrivateKey,
        environment: Environment = Environment.DEMO,
    ):
        """Initializes the client with the provided API key and private key.

        Args:
            key_id (str): Your Kalshi API key ID.
            private_key (rsa.RSAPrivateKey): Your RSA private key.
            environment (Environment): The API environment to use (DEMO or PROD).
        """
        self.key_id = key_id
        self.private_key = private_key
        self.environment = environment
        self.last_api_call = datetime.now()

        if self.environment == Environment.DEMO:
            self.HTTP_BASE_URL = "https://demo-api.kalshi.co"
            self.WS_BASE_URL = "wss://demo-api.kalshi.co"
        elif self.environment == Environment.PROD:
            self.HTTP_BASE_URL = "https://api.elections.kalshi.com"
            self.WS_BASE_URL = "wss://api.elections.kalshi.com"
        else:
            raise ValueError("Invalid environment")

    def request_headers(self, method: str, path: str) -> Dict[str, Any]:
        """Generate Kalshi authentication headers for a request.

        Args:
            method: HTTP method in uppercase (for example, ``GET`` or ``POST``).
            path: URL path beginning with ``/``. Query params may be present.

        Returns:
            Dict[str, Any]: Request headers including key ID, signature, and timestamp.

        Raises:
            ValueError: If request signing fails.
        """
        current_time_milliseconds = int(time.time() * 1000)
        timestamp_str = str(current_time_milliseconds)

        # Remove query params from path
        path_parts = path.split('?')

        msg_string = timestamp_str + method + path_parts[0]
        signature = self.sign_pss_text(msg_string)

        headers = {
            "Content-Type": "application/json",
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_str,
        }
        return headers

    def sign_pss_text(self, text: str) -> str:
        """Sign text with RSA-PSS and return the Base64 signature.

        Args:
            text: Canonical message string to sign.

        Returns:
            str: Base64-encoded signature.

        Raises:
            ValueError: If RSA signing fails.
        """
        message = text.encode('utf-8')
        try:
            signature = self.private_key.sign(
                message,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.DIGEST_LENGTH
                ),
                hashes.SHA256()
            )
            return base64.b64encode(signature).decode('utf-8')
        except InvalidSignature as e:
            raise ValueError("RSA sign PSS failed") from e

class KalshiHttpClient(KalshiBaseClient):
    """Authenticated HTTP client for Kalshi REST endpoints."""
    DEFAULT_TIMEOUT_SECONDS = 15.0

    def __init__(
        self,
        key_id: str,
        private_key: rsa.RSAPrivateKey,
        environment: Environment = Environment.DEMO,
    ):
        """Initialize HTTP client session and endpoint prefixes.

        Args:
            key_id: Kalshi API key ID.
            private_key: RSA private key used for request signing.
            environment: Target API environment.
        """
        super().__init__(key_id, private_key, environment)
        self.host = self.HTTP_BASE_URL
        self.exchange_url = "/trade-api/v2/exchange"
        self.markets_url = "/trade-api/v2/markets"
        self.portfolio_url = "/trade-api/v2/portfolio"
        self.session = requests.Session()

    def rate_limit(self) -> None:
        """Apply a minimum pause between API calls.

        This client-side guard enforces a small gap between outgoing requests
        to reduce burst behavior and lower the chance of server rate limiting.

        Returns:
            None
        """
        THRESHOLD_IN_MILLISECONDS = 100
        now = datetime.now()
        threshold_in_microseconds = 1000 * THRESHOLD_IN_MILLISECONDS
        threshold_in_seconds = THRESHOLD_IN_MILLISECONDS / 1000
        if now - self.last_api_call < timedelta(microseconds=threshold_in_microseconds):
            time.sleep(threshold_in_seconds)
        self.last_api_call = datetime.now()

    def raise_if_bad_response(self, response: requests.Response) -> None:
        """Raise ``HTTPError`` for non-2xx responses.

        Args:
            response: Raw ``requests`` response object.

        Returns:
            None

        Raises:
            requests.HTTPError: If status code is outside the ``2xx`` range.
        """
        if response.status_code not in range(200, 299):
            response.raise_for_status()

    @staticmethod
    def normalize_path(path: str) -> str:
        """Ensure request path starts with ``/``.

        Args:
            path: Relative API path with or without leading slash.

        Returns:
            str: Normalized path that starts with ``/``.
        """
        return path if path.startswith("/") else f"/{path}"

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> Any:
        """Execute an authenticated HTTP request.

        Args:
            method: HTTP method (``GET``, ``POST``, ``DELETE``).
            path: API endpoint path.
            params: Optional query-string parameters.
            body: Optional JSON request body.
            timeout_seconds: Per-request timeout.

        Returns:
            Any: Parsed JSON response.

        Raises:
            requests.RequestException: If request transmission fails.
            requests.HTTPError: If the server returns non-2xx status.
            ValueError: If authentication signing fails.
        """
        normalized_path = self.normalize_path(path)
        self.rate_limit()
        response = self.session.request(
            method=method,
            url=self.host + normalized_path,
            headers=self.request_headers(method, normalized_path),
            params=params,
            json=body,
            timeout=timeout_seconds,
        )
        self.raise_if_bad_response(response)
        return response.json()

    def post(self, path: str, body: dict, timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS) -> Any:
        """Perform an authenticated ``POST`` request.

        Args:
            path: API endpoint path.
            body: JSON request body.
            timeout_seconds: Per-request timeout.

        Returns:
            Any: Parsed JSON response.

        Raises:
            requests.RequestException: If request transmission fails.
            requests.HTTPError: If the server returns non-2xx status.
        """
        return self._request("POST", path, body=body, timeout_seconds=timeout_seconds)

    def get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> Any:
        """Perform an authenticated ``GET`` request.

        Args:
            path: API endpoint path.
            params: Optional query-string parameters.
            timeout_seconds: Per-request timeout.

        Returns:
            Any: Parsed JSON response.

        Raises:
            requests.RequestException: If request transmission fails.
            requests.HTTPError: If the server returns non-2xx status.
        """
        return self._request("GET", path, params=params, timeout_seconds=timeout_seconds)

    def delete(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> Any:
        """Perform an authenticated ``DELETE`` request.

        Args:
            path: API endpoint path.
            params: Optional query-string parameters.
            timeout_seconds: Per-request timeout.

        Returns:
            Any: Parsed JSON response.

        Raises:
            requests.RequestException: If request transmission fails.
            requests.HTTPError: If the server returns non-2xx status.
        """
        return self._request("DELETE", path, params=params, timeout_seconds=timeout_seconds)

    def get_balance(self, timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS) -> Dict[str, Any]:
        """Retrieve portfolio balance.

        Args:
            timeout_seconds: Per-request timeout.

        Returns:
            Dict[str, Any]: Balance payload returned by Kalshi.
        """
        return self.get(self.portfolio_url + '/balance', timeout_seconds=timeout_seconds)

    def get_exchange_status(self, timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS) -> Dict[str, Any]:
        """Retrieve exchange status metadata.

        Args:
            timeout_seconds: Per-request timeout.

        Returns:
            Dict[str, Any]: Exchange status payload.
        """
        return self.get(self.exchange_url + "/status", timeout_seconds=timeout_seconds)

    def get_trades(
        self,
        ticker: Optional[str] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        max_ts: Optional[int] = None,
        min_ts: Optional[int] = None,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> Dict[str, Any]:
        """Retrieve market trades filtered by optional constraints.

        Args:
            ticker: Optional market ticker to filter trades.
            limit: Optional max number of records to return.
            cursor: Optional pagination cursor.
            max_ts: Optional upper Unix timestamp bound.
            min_ts: Optional lower Unix timestamp bound.
            timeout_seconds: Per-request timeout.

        Returns:
            Dict[str, Any]: Trades payload.
        """
        params = {
            'ticker': ticker,
            'limit': limit,
            'cursor': cursor,
            'max_ts': max_ts,
            'min_ts': min_ts,
        }
        # Remove None values
        params = {k: v for k, v in params.items() if v is not None}
        return self.get(self.markets_url + '/trades', params=params, timeout_seconds=timeout_seconds)

    def get_path(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> Dict[str, Any]:
        """Retrieve data from an arbitrary authenticated ``GET`` endpoint.

        Args:
            path: API endpoint path.
            params: Optional query-string parameters.
            timeout_seconds: Per-request timeout.

        Returns:
            Dict[str, Any]: Endpoint payload parsed from JSON.
        """
        return self.get(path, params=params, timeout_seconds=timeout_seconds)

class KalshiWebSocketClient(KalshiBaseClient):
    """Authenticated WebSocket client for Kalshi streaming endpoints."""
    def __init__(
        self,
        key_id: str,
        private_key: rsa.RSAPrivateKey,
        environment: Environment = Environment.DEMO,
    ):
        """Initialize WebSocket client state.

        Args:
            key_id: Kalshi API key ID.
            private_key: RSA private key used for request signing.
            environment: Target API environment.
        """
        super().__init__(key_id, private_key, environment)
        self.ws = None
        self.url_suffix = "/trade-api/ws/v2"
        self.message_id = 1  # Add counter for message IDs

    async def connect(self):
        """Open authenticated WebSocket connection and process messages.

        Returns:
            None

        Raises:
            Exception: Propagates connection and runtime websocket exceptions.
        """
        host = self.WS_BASE_URL + self.url_suffix
        auth_headers = self.request_headers("GET", self.url_suffix)
        async with websockets.connect(host, additional_headers=auth_headers) as websocket:
            self.ws = websocket
            await self.on_open()
            await self.handler()

    async def on_open(self):
        """Handle connection-open event and send initial subscriptions.

        Returns:
            None
        """
        print("WebSocket connection opened.")
        await self.subscribe_to_tickers()

    async def subscribe_to_tickers(self):
        """Subscribe to ticker channel updates.

        Returns:
            None

        Raises:
            Exception: If websocket send fails.
        """
        subscription_message = {
            "id": self.message_id,
            "cmd": "subscribe",
            "params": {
                "channels": ["ticker"]
            }
        }
        await self.ws.send(json.dumps(subscription_message))
        self.message_id += 1

    async def handler(self):
        """Consume incoming messages until the connection closes.

        Returns:
            None
        """
        try:
            async for message in self.ws:
                await self.on_message(message)
        except websockets.ConnectionClosed as e:
            await self.on_close(e.code, e.reason)
        except Exception as e:
            await self.on_error(e)

    async def on_message(self, message):
        """Callback for each received message.

        Args:
            message: Raw message payload from the websocket stream.

        Returns:
            None
        """
        print("Received message:", message)

    async def on_error(self, error):
        """Callback for websocket runtime errors.

        Args:
            error: Exception encountered during websocket processing.

        Returns:
            None
        """
        print("WebSocket error:", error)

    async def on_close(self, close_status_code, close_msg):
        """Callback when websocket connection closes.

        Args:
            close_status_code: WebSocket close status code.
            close_msg: Server-provided close reason message.

        Returns:
            None
        """
        print("WebSocket connection closed with code:", close_status_code, "and message:", close_msg)
