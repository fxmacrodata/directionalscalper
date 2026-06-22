"""Aster V3 (API Wallet / Web3-signed) connector.

Aster announced V1 (HMAC API Key + Secret) deprecation on 2026-03-25 — new V1
keys can no longer be created. V3 uses an API Wallet model:

  - `user`        master account wallet address
  - `signer`      API wallet address  (https://www.asterdex.com/en/api-wallet)
  - `private_key` private key for the signer (signs EIP-712 typed data)

Signing flow (matches the reference impl in
https://github.com/asterdex/api-docs/blob/master/V3(Recommended)/EN/aster-finance-futures-api-v3.md):

  1. params = {business_params..., 'nonce': <microsecond>, 'signer': <signer_addr>}
  2. encoded = urllib.parse.urlencode(params)
  3. typed_data = EIP-712 wrapping `{msg: encoded}` under domain
     {name: "AsterSignTransaction", version: "1", chainId: 1666,
      verifyingContract: "0x0000000000000000000000000000000000000000"}
  4. sig = ECDSA sign(typed_data) with private_key
  5. send POST/DELETE/GET to https://fapi3.asterdex.com<path>?<encoded>&signature=<hex>

Most V1 endpoints have a V3 equivalent that's just `/fapi/v1/` → `/fapi/v3/`. The
one exception we hit: V1's `/fapi/v1/account` (which returns both balance and
positions) maps to V3's `/fapi/v3/accountWithJoinMargin` (identical response
shape). The path-rewrite table below handles that and any other irregular
mappings.

This subclass overrides only the auth/URL layer; all the precision-cache,
hedge-mode, position parsing, orderbook parsing, kline parsing, etc. from
AsterExchange (V1) are reused unchanged because they operate on response
shapes that V3 has kept identical.

Config:
  {
    "name": "aster_v3",
    "user": "0x<master_wallet_address>",
    "signer": "0x<api_wallet_address>",
    "private_key": "0x<api_wallet_private_key>",
    "websocket_data": true,
    "testnet": false
  }
"""

from __future__ import annotations

import logging
import threading
import time
import urllib.parse
from typing import Any, Dict, Optional

import requests

from core.exchange.aster import AsterExchange

try:
    from eth_account import Account
    from eth_account.messages import encode_typed_data
    ETH_AVAILABLE = True
except Exception:  # pragma: no cover
    ETH_AVAILABLE = False


# EIP-712 domain reverse-engineered from the V3 reference impl. The
# verifyingContract is the zero address (HL-style — domain separator alone
# is enough, no on-chain contract verifies the sig).
V3_DOMAIN = {
    "name": "AsterSignTransaction",
    "version": "1",
    "chainId": 1666,
    "verifyingContract": "0x0000000000000000000000000000000000000000",
}

V3_TYPES = {
    "Message": [{"name": "msg", "type": "string"}],
}

V3_BASE_URL = "https://fapi3.asterdex.com"

# V1 path → V3 path. Most go from /fapi/v1/X to /fapi/v3/X — handled by the
# default rewrite. List only the ones that don't follow that rule.
V3_PATH_OVERRIDES = {
    # V1 /account was balance+positions+assets in one shot. V3 splits balance
    # into /balance (lightweight) and full info into /accountWithJoinMargin
    # (same shape as V1 /account). Use the full one so the existing
    # get_balance + get_positions parsing keeps working.
    "/fapi/v1/account": "/fapi/v3/accountWithJoinMargin",
}


def _v3_path(path: str) -> str:
    """Rewrite a V1 path to its V3 equivalent."""
    if path in V3_PATH_OVERRIDES:
        return V3_PATH_OVERRIDES[path]
    if path.startswith("/fapi/v1/"):
        return "/fapi/v3/" + path[len("/fapi/v1/"):]
    return path


class AsterExchangeV3(AsterExchange):
    """Aster V3 connector. Inherits all market-data + hedge-mode logic from
    the V1 connector; overrides only auth + base URL + path rewriting."""

    def __init__(self, config: Dict, logger=None):
        if not ETH_AVAILABLE:
            raise RuntimeError(
                "eth_account is required for Aster V3 signing. Install via:\n"
                "    pip install eth-account"
            )
        # Stash V3 credentials before super().__init__ runs (parent doesn't
        # need api_key/api_secret — we pass empty placeholders to satisfy
        # any reads of self.config).
        self._v3_user = (config.get("user") or "").lower()
        self._v3_signer = (config.get("signer") or "").lower()
        self._v3_private_key = config.get("private_key") or ""

        # Parent __init__ reads config['api_key']/['api_secret'] only inside
        # connect() — but to be safe, provide stub values so any other readers
        # don't crash. The V3 connect() override below skips those reads.
        cfg = dict(config)
        cfg.setdefault("api_key", "V3_NO_KEY")
        cfg.setdefault("api_secret", "V3_NO_SECRET")
        super().__init__(cfg, logger)

        if not self._v3_signer:
            raise ValueError("aster_v3 config: 'signer' (API wallet address) is required")
        if not self._v3_private_key:
            raise ValueError("aster_v3 config: 'private_key' is required")
        if not self._v3_user:
            # If user is omitted, default to signer for self-signing setups.
            self._v3_user = self._v3_signer

        # Override base URL and ID. The dedicated V3 host (fapi3.asterdex.com)
        # is geo/IP-blocked from some datacenters (AWS-ELB 403 on EVERYTHING,
        # incl. the public ping) — but the V1 host (fapi.asterdex.com) serves
        # the SAME /fapi/v3/* paths and is not blocked. So allow a config
        # override; default stays fapi3 for compatibility.
        self.base_url = config.get("v3_base_url") or V3_BASE_URL
        self.id = "aster_v3"

        # Build signer account once.
        self._v3_account = Account.from_key(self._v3_private_key)
        signer_derived = self._v3_account.address.lower()
        if signer_derived != self._v3_signer:
            self.logger.warning(
                f"aster_v3: 'signer' in config ({self._v3_signer}) does not match "
                f"the address derived from private_key ({signer_derived}). Using derived."
            )
            self._v3_signer = signer_derived

        # Nonce generator state (microseconds + ordering counter, matches
        # the reference impl).
        self._nonce_lock = threading.Lock()
        self._nonce_last_ms = 0
        self._nonce_counter = 0

    # -------------------------------------------------------------------------
    # Nonce + signing
    # -------------------------------------------------------------------------

    def _get_nonce(self) -> int:
        """Microsecond-precision nonce with a same-ms ordering counter so two
        requests in the same wall-clock millisecond still get distinct,
        increasing nonces (server rejects duplicates / past nonces)."""
        with self._nonce_lock:
            now_ms = int(time.time())
            if now_ms == self._nonce_last_ms:
                self._nonce_counter += 1
            else:
                self._nonce_last_ms = now_ms
                self._nonce_counter = 0
            return now_ms * 1_000_000 + self._nonce_counter

    def _v3_sign_params(self, params: Dict[str, Any]) -> str:
        """Sign the URL-encoded params via EIP-712, return signature hex.

        Mirrors the reference impl: builds {msg: urlencoded(params)} under the
        AsterSignTransaction domain and signs with the API wallet private key.
        """
        encoded = urllib.parse.urlencode(params)
        signed = Account.sign_message(
            encode_typed_data(
                domain_data=V3_DOMAIN,
                message_types=V3_TYPES,
                message_data={"msg": encoded},
            ),
            self._v3_private_key,
        )
        sig_hex = signed.signature.hex()
        if not sig_hex.startswith("0x"):
            sig_hex = "0x" + sig_hex
        return sig_hex

    # -------------------------------------------------------------------------
    # Request layer — override to do V3 signing + path rewrite
    # -------------------------------------------------------------------------

    def _public_request(self, endpoint: str, params: Dict = None) -> Dict:
        """V3 public endpoints sit at /fapi/v3/* (same shape, different prefix)."""
        v3_endpoint = _v3_path(endpoint)
        url = f"{self.base_url}{v3_endpoint}"
        try:
            r = self._session_get(url, params or {}, timeout=10)
            return r
        except Exception as e:
            self.logger.error(f"aster_v3 public {v3_endpoint}: {e}")
            return {}

    def _session_get(self, url: str, params: Dict, timeout: float = 10.0) -> Dict:
        """Bare GET helper that uses self.session if available, else fresh requests."""
        sess = getattr(self, "session", None) or requests
        r = sess.get(url, params=params, timeout=timeout, headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "DirectionalScalper/aster_v3",
        })
        r.raise_for_status()
        try:
            return r.json()
        except ValueError:
            return {}

    def _signed_request(self, method: str, endpoint: str, params: Dict = None) -> Dict:
        """V3 signed request: add nonce + signer, EIP-712 sign, send.

        The reference impl puts everything (params + signature) in the URL
        query string and sends an empty body. We do the same — keeps it
        identical to the documented working flow.
        """
        # No broker/affiliate ID for Aster
        method = method.upper()
        v3_endpoint = _v3_path(endpoint)
        url = f"{self.base_url}{v3_endpoint}"
        body = dict(params or {})
        body["nonce"] = str(self._get_nonce())
        body["signer"] = self._v3_signer
        # `user` is only required by some endpoints; include it always so the
        # subset that needs it works. Endpoints that don't expect it ignore.
        if "user" not in body and self._v3_user:
            body["user"] = self._v3_user

        try:
            sig = self._v3_sign_params(body)
        except Exception as e:
            self.logger.error(f"aster_v3 sign failure ({v3_endpoint}): {e}")
            return {}

        encoded = urllib.parse.urlencode(body)
        full_url = f"{url}?{encoded}&signature={sig}"
        sess = getattr(self, "session", None) or requests
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "DirectionalScalper/aster_v3",
        }
        try:
            if method == "GET":
                r = sess.get(full_url, timeout=10, headers=headers)
            elif method == "POST":
                r = sess.post(full_url, timeout=10, headers=headers)
            elif method == "DELETE":
                r = sess.delete(full_url, timeout=10, headers=headers)
            elif method == "PUT":
                r = sess.put(full_url, timeout=10, headers=headers)
            else:
                raise ValueError(f"unsupported method {method}")
            # Try to JSON-decode; bubble error responses up so callers can
            # see what the server complained about.
            try:
                data = r.json()
            except ValueError:
                data = {"raw": r.text, "status": r.status_code}
            # V3 returns {"code": N, "msg": "..."} on failure; pass through
            # so callers can log and decide. Don't raise — matches V1 behavior.
            return data
        except Exception as e:
            self.logger.error(f"aster_v3 {method} {v3_endpoint}: {e}")
            return {}

    # -------------------------------------------------------------------------
    # Connect — override to skip V1's auth-header session setup
    # -------------------------------------------------------------------------

    def connect(self) -> bool:
        """V3 doesn't use an X-MBX-APIKEY header — auth lives in each request's
        signature. We still create a session for connection reuse + run the
        precision cache load."""
        try:
            self.session = requests.Session()
            # Optimistic connect probe — V3 /ping is unsigned, cheap, no auth.
            try:
                ping = self._public_request("/fapi/v1/ping")  # rewritten to /fapi/v3/ping
                self.logger.info(f"aster_v3 ping: {ping if ping else 'no response'}")
            except Exception as e:
                self.logger.warning(f"aster_v3 ping warning (continuing anyway): {e}")

            # Validate signing works by hitting an authenticated endpoint
            # that's cheap: GET /fapi/v3/balance. If signing is wrong, we'll
            # see {code: -xxxx, msg: "..."} and bail.
            balance_probe = self._signed_request("GET", "/fapi/v3/balance")
            if isinstance(balance_probe, dict) and balance_probe.get("code") not in (None, 200):
                self.logger.error(
                    f"aster_v3 signing probe failed: {balance_probe}. "
                    f"Check user/signer/private_key alignment."
                )
                return False

            # Pre-load exchange info (populates _precision_cache +
            # _market_qty_steps / tick_sizes / min_order_values via the
            # parent's _load_exchange_info — which uses _public_request
            # → automatically routed to /fapi/v3/exchangeInfo).
            self._load_exchange_info()
            self.load_markets()

            # WebSocket data streams — Aster V3 WS endpoints are still
            # wss://fstream.asterdex.com (public WS works on master URL).
            # Reuse the parent's V1 WS manager — the WS protocol hasn't
            # changed in the docs.
            if self.websocket_data_enabled:
                # Lazily import so the V3 file can be loaded without WS deps.
                from core.exchange.aster import AsterWebSocketDataManager, WEBSOCKET_AVAILABLE
                if not WEBSOCKET_AVAILABLE:
                    self.logger.warning("aster_v3: websocket-client not installed; WS disabled")
                else:
                    self._ws_data = AsterWebSocketDataManager(
                        api_key="V3_NO_KEY",  # public streams only — no listenKey needed
                        api_secret="V3_NO_SECRET",
                        base_url="https://fapi.asterdex.com",  # WS auth URL stays at V1 for now
                        testnet=self.config.get("testnet", False),
                        logger=self.logger,
                    )
                    if self._ws_data.connect():
                        self.logger.info(
                            "aster_v3 WS data streams ENABLED (public-only: l2/trades/bookTicker)"
                        )
                    else:
                        self.logger.warning(
                            "aster_v3 WS data streams slow to connect — REST fallback active"
                        )
                    self.rate_limit_delay = 0.01

            self.logger.info(
                f"aster_v3 connected: user={self._v3_user[:10]}... "
                f"signer={self._v3_signer[:10]}... "
                f"perp_symbols_cached={len(self._market_qty_steps)}"
            )
            return True
        except Exception as e:
            self.logger.error(f"aster_v3 connect failed: {e}", exc_info=True)
            return False
