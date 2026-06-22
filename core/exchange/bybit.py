"""
Bybit Exchange Implementation - Institutional Grade

WebSocket Order Support:
  - Enable with config: "websocket_orders": true
  - Uses Bybit Trade WebSocket (wss://stream.bybit.com/v5/trade)
  - orderLinkId for idempotency (exchange-enforced deduplication)
  - Full latency logging (ms precision)
  - Order state tracking (PENDING → SENT → CONFIRMED)
"""

import ccxt
import math
import time
import threading
import uuid
import json
import hmac
import hashlib
import websocket
from enum import Enum
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Callable
from .base import BaseExchange


class OrderState(Enum):
    """Order lifecycle states for institutional tracking"""
    PENDING = "pending"      # Generated, not yet sent
    SENT = "sent"           # Sent to exchange, awaiting response
    CONFIRMED = "confirmed"  # Exchange confirmed placement
    REJECTED = "rejected"    # Exchange rejected
    FILLED = "filled"       # Fully filled
    PARTIAL = "partial"     # Partially filled
    CANCELLED = "cancelled"  # Cancelled
    TIMEOUT = "timeout"     # Response timeout (unknown state)

# Check if websocket-client is available
try:
    import websocket
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False


class WebSocketOrderManager:
    """
    Manages WebSocket connection to Bybit Trade API for fast order placement.
    Uses wss://stream.bybit.com/v5/trade endpoint.
    Falls back to REST if WS is unavailable.
    """

    MAINNET_URL = "wss://stream.bybit.com/v5/trade"
    TESTNET_URL = "wss://stream-testnet.bybit.com/v5/trade"

    def __init__(self, api_key: str, api_secret: str, testnet: bool = False, logger=None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        self.logger = logger
        self.ws = None
        self.connected = False
        self._responses = {}  # Track responses by reqId
        self._lock = threading.Lock()
        self._recv_thread = None
        self._running = False
        self._reconnect_backoff = 1.0  # exponential backoff seconds
        self._max_reconnect_backoff = 30.0
        self._last_reconnect_attempt = 0.0
        # Watchdog for silent dead-pipe scenario: WS appears connected but
        # writes time out without raising connection errors (Bybit-side
        # backend hiccup or socket-level death). Counts consecutive write
        # timeouts; force-reconnects when threshold is hit.
        self._consecutive_write_failures = 0
        self._write_failure_threshold = 3
        self._reconnect_lock = threading.Lock()
        # Tick size cache: "ASTERUSDT" -> 0.0001, "DOGEUSDT" -> 0.00001
        self._tick_sizes: Dict[str, float] = {}
        self._qty_steps: Dict[str, float] = {}

    def set_precision(self, bybit_symbol: str, tick_size: float, qty_step: float):
        """Cache tick size and qty step for a symbol."""
        self._tick_sizes[bybit_symbol] = tick_size
        self._qty_steps[bybit_symbol] = qty_step

    def _snap_price(self, symbol: str, price: float) -> str:
        """Snap price to tick size and return as string for Bybit API."""
        bybit_symbol = symbol.replace("/", "").replace(":USDT", "")
        tick = self._tick_sizes.get(bybit_symbol)
        if tick and tick > 0:
            # Round to nearest tick
            price = round(round(price / tick) * tick, 10)
            # Derive decimal places from tick size (handles 1e-05 etc.)
            decimals = max(0, -int(math.floor(math.log10(tick) + 1e-9)))
            return f"{price:.{decimals}f}"
        # Fallback: 8 decimal places, strip trailing zeros
        return f"{price:.8f}".rstrip('0').rstrip('.')

    def _snap_qty(self, symbol: str, qty: float) -> int:
        """Snap qty to qty step (integer for most Bybit perps)."""
        bybit_symbol = symbol.replace("/", "").replace(":USDT", "")
        step = self._qty_steps.get(bybit_symbol, 1.0)
        if step >= 1.0:
            rounded = round(qty)
            return max(1, rounded)
        # Sub-unit step sizes (rare for perps)
        snapped = round(round(qty / step) * step, 8)
        return max(step, snapped)

    def _format_qty(self, symbol: str, snapped_qty) -> str:
        """Format a snapped qty as a string suitable for Bybit's API.

        For integer-step symbols (e.g. XRP qty_step=1) → "29".
        For sub-unit-step symbols (e.g. LINK qty_step=0.1) → "29.0", "0.7",
        with a decimal count derived from the step (preserves the trailing
        decimal so Bybit doesn't reject the qty as if it were integer-only).
        """
        import math
        bybit_symbol = symbol.replace("/", "").replace(":USDT", "")
        step = self._qty_steps.get(bybit_symbol, 1.0)
        if step >= 1.0:
            try:
                return str(int(snapped_qty))
            except (TypeError, ValueError):
                return str(snapped_qty)
        # Determine decimal places from the step (0.1 → 1, 0.001 → 3, etc.)
        decimals = max(0, -int(math.floor(math.log10(step))))
        return f"{float(snapped_qty):.{decimals}f}"

    def _generate_signature(self, expires: int) -> str:
        """Generate HMAC SHA256 signature for WebSocket authentication.

        Bybit format: sign the string "GET/realtime{expires}"
        See: https://bybit-exchange.github.io/docs/v5/ws/connect
        """
        param_str = f"GET/realtime{expires}"
        return hmac.new(
            self.api_secret.encode('utf-8'),
            param_str.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

    def connect(self) -> bool:
        """Connect to Bybit Trade WebSocket and authenticate."""
        if not WEBSOCKET_AVAILABLE:
            if self.logger:
                self.logger.warning("websocket-client not installed - WebSocket orders disabled")
            return False

        try:
            url = self.TESTNET_URL if self.testnet else self.MAINNET_URL
            self.ws = websocket.create_connection(url, timeout=10)

            # Authenticate - expires must be a FUTURE timestamp
            expires = int(time.time() * 1000) + 10000  # 10 seconds in future
            signature = self._generate_signature(expires)

            auth_msg = {
                "op": "auth",
                "args": [self.api_key, expires, signature]
            }
            self.ws.send(json.dumps(auth_msg))

            # Wait for auth response
            response = json.loads(self.ws.recv())
            if response.get("success") or response.get("retCode") == 0:
                self.connected = True
                self._running = True
                # Start background receiver thread
                self._recv_thread = threading.Thread(target=self._receive_loop, daemon=True)
                self._recv_thread.start()
                if self.logger:
                    self.logger.info("WebSocket Trade API connected and authenticated")
                return True
            else:
                if self.logger:
                    self.logger.error(f"WebSocket auth failed: {response}")
                self.ws.close()
                return False

        except Exception as e:
            if self.logger:
                self.logger.error(f"WebSocket connection failed: {e}")
            self.connected = False
            return False

    def _receive_loop(self):
        """Background thread to receive WebSocket messages with auto-reconnect."""
        while self._running:
            try:
                if not self.ws:
                    break
                msg = self.ws.recv()
                if msg:
                    data = json.loads(msg)
                    req_id = data.get("reqId")
                    if req_id:
                        with self._lock:
                            self._responses[req_id] = data
                    # Reset backoff on successful message
                    self._reconnect_backoff = 1.0
            except websocket.WebSocketTimeoutException:
                continue
            except Exception as e:
                if not self._running:
                    break
                if self.logger:
                    self.logger.warning(f"WS Trade receive error: {e}")
                self.connected = False
                # Attempt reconnection loop
                while self._running:
                    if self.logger:
                        self.logger.info(
                            f"WS Trade reconnecting in {self._reconnect_backoff:.0f}s...")
                    time.sleep(self._reconnect_backoff)
                    self._reconnect_backoff = min(
                        self._reconnect_backoff * 2, self._max_reconnect_backoff)
                    if self._try_reconnect():
                        if self.logger:
                            self.logger.info("WS Trade reconnected successfully")
                        break
                continue

        self.connected = False

    def _note_write_failure(self) -> None:
        """Track consecutive WS write timeouts. Force-reconnect on threshold.

        The receive loop only reconnects when recv() raises — but a dead-pipe
        scenario (Bybit backend hiccup, silent socket death) lets writes hang
        forever without raising on recv. This watchdog detects that pattern
        from the write side and forces a reconnection.
        """
        with self._reconnect_lock:
            self._consecutive_write_failures += 1
            count = self._consecutive_write_failures
            if self.logger:
                self.logger.warning(
                    f"[WS-WATCHDOG] consecutive write failures: {count}/{self._write_failure_threshold}"
                )
            if count < self._write_failure_threshold:
                return
            # Threshold hit — force-reconnect. Reset counter so next batch
            # of failures triggers another reconnect attempt if needed.
            self._consecutive_write_failures = 0
            if self.logger:
                self.logger.error(
                    f"[WS-WATCHDOG] Threshold {self._write_failure_threshold} hit — "
                    f"forcing WS Trade reconnect to recover from dead pipe"
                )
            self.connected = False
            try:
                if self.ws:
                    self.ws.close()
            except Exception:
                pass
            self.ws = None
            # Try to reconnect synchronously. _receive_loop's reconnect path
            # also fires when recv() finally raises after our close().
            ok = self._try_reconnect()
            if self.logger:
                if ok:
                    self.logger.info("[WS-WATCHDOG] Force-reconnect SUCCEEDED")
                else:
                    self.logger.warning(
                        "[WS-WATCHDOG] Force-reconnect failed; receive loop will retry with backoff"
                    )

    def _try_reconnect(self) -> bool:
        """Attempt to reconnect the WS Trade connection. Called from _receive_loop."""
        try:
            if self.ws:
                try:
                    self.ws.close()
                except Exception:
                    pass
                self.ws = None

            url = self.TESTNET_URL if self.testnet else self.MAINNET_URL
            self.ws = websocket.create_connection(url, timeout=10)

            expires = int(time.time() * 1000) + 10000
            signature = self._generate_signature(expires)
            auth_msg = {
                "op": "auth",
                "args": [self.api_key, expires, signature]
            }
            self.ws.send(json.dumps(auth_msg))

            response = json.loads(self.ws.recv())
            if response.get("success") or response.get("retCode") == 0:
                self.connected = True
                return True
            else:
                if self.logger:
                    self.logger.warning(f"WS Trade reconnect auth failed: {response}")
                self.ws.close()
                self.ws = None
                return False
        except Exception as e:
            if self.logger:
                self.logger.warning(f"WS Trade reconnect failed: {e}")
            self.ws = None
            return False

    def _wait_for_response(self, req_id: str, timeout: float = 5.0) -> Optional[Dict]:
        """Wait for a response with the given reqId."""
        start = time.time()
        while time.time() - start < timeout:
            with self._lock:
                if req_id in self._responses:
                    return self._responses.pop(req_id)
            time.sleep(0.01)
        return None

    def disconnect(self):
        """Disconnect WebSocket"""
        self._running = False
        if self.ws:
            try:
                self.ws.close()
            except:
                pass
            self.ws = None
        self.connected = False

    def place_order(
        self,
        symbol: str,
        side: str,
        amount: float,
        price: float = None,
        order_type: str = "Limit",
        reduce_only: bool = False,
        post_only: bool = False,
        position_idx: int = 0,
        order_link_id: str = None  # Institutional: client order ID for idempotency
    ) -> Dict:
        """
        Place order via WebSocket Trade API (faster than REST).

        INSTITUTIONAL FEATURES:
        - orderLinkId for idempotency (exchange rejects duplicates)
        - Latency logging (ms precision)
        - Full order tracking

        Returns order dict or empty dict on failure.
        """
        if not self.connected or not self.ws:
            return {}

        # === LATENCY TRACKING START ===
        t_start = time.perf_counter()

        try:
            # Convert symbol format: "ASTER/USDT:USDT" -> "ASTERUSDT"
            bybit_symbol = symbol.replace("/", "").replace(":USDT", "")
            req_id = str(uuid.uuid4())[:8]
            timestamp = str(int(time.time() * 1000))

            # Snap qty to the symbol's actual lot step (from market precision).
            # Earlier this path hardcoded round-to-integer with the assumption
            # that "most perp symbols have qtyStep=1" — wrong for LINK (0.1),
            # SOL (0.1), 1000-coin tokens, etc. Using _snap_qty honors the
            # exchange's real qtyStep so 0.7 LINK stays 0.7 LINK instead of
            # bloating to 1 LINK ($9.40 vs $6.58 of actual notional).
            snapped_qty = self._snap_qty(symbol, amount)
            qty_str = self._format_qty(symbol, snapped_qty)

            order_args = {
                "category": "linear",
                "symbol": bybit_symbol,
                "side": side.capitalize(),
                "orderType": order_type.capitalize(),
                "qty": qty_str,
                "positionIdx": position_idx,
            }

            # === INSTITUTIONAL: orderLinkId for idempotency ===
            if order_link_id:
                order_args["orderLinkId"] = order_link_id

            if price and order_type.lower() == "limit":
                order_args["price"] = self._snap_price(symbol, price)

            if reduce_only:
                order_args["reduceOnly"] = True

            if post_only and order_type.lower() == "limit":
                order_args["timeInForce"] = "PostOnly"
            else:
                order_args["timeInForce"] = "GTC"

            msg = {
                "reqId": req_id,
                "op": "order.create",
                "header": {
                    "X-BAPI-TIMESTAMP": timestamp,
                    "X-BAPI-RECV-WINDOW": "5000",
                    "Referer": "Nu000450"  # Broker ID for affiliate tracking
                },
                "args": [order_args]
            }

            if self.logger:
                rounded_note = f" (rounded from {amount})" if snapped_qty != amount else ""
                link_note = f" linkId={order_link_id}" if order_link_id else ""
                price_str = order_args.get("price", price)
                self.logger.info(f"[WS-SEND] {side} {qty_str}{rounded_note} @ {price_str}{link_note}")

            self.ws.send(json.dumps(msg))
            response = self._wait_for_response(req_id, timeout=3.0)

            # === LATENCY TRACKING END ===
            t_end = time.perf_counter()
            latency_ms = (t_end - t_start) * 1000

            if response and response.get("retCode") == 0:
                order_data = response.get("data", {})
                order_id = order_data.get('orderId', req_id)
                if self.logger:
                    self.logger.info(f"[WS-OK] orderId={order_id} latency={latency_ms:.1f}ms")
                return {
                    'id': order_id,
                    'clientOrderId': order_link_id,
                    'symbol': symbol,
                    'side': side,
                    'amount': amount,
                    'price': price,
                    'status': 'open',
                    'latency_ms': round(latency_ms, 1)
                }
                # Watchdog: any successful write resets the failure counter
                self._consecutive_write_failures = 0
            else:
                ret_code = response.get("retCode") if response else "NO_RESPONSE"
                ret_msg = response.get("retMsg", "timeout") if response else "timeout"
                if self.logger:
                    self.logger.warning(f"[WS-FAIL] {side} {amount} @ {price} | code={ret_code} msg={ret_msg} latency={latency_ms:.1f}ms")
                # SAFETY NET (Bybit 110017): a reduce-only order whose qty would be
                # "truncated to zero" means the REAL position is already flat (or
                # below the lot step). Surface it as a DISTINCT already-flat
                # outcome instead of a bare {} timeout, so the strategy treats it
                # as "position already closed" — clears the pending order/level and
                # STOPS re-posting it (no retry loop), rather than retrying blind.
                if ret_code == 110017:
                    return {"error": "reduce_only_on_flat", "retCode": 110017}
                return {}

        except Exception as e:
            t_end = time.perf_counter()
            latency_ms = (t_end - t_start) * 1000
            if self.logger:
                self.logger.error(f"[WS-ERROR] {side} {amount} @ {price} | error={e} latency={latency_ms:.1f}ms")
            # Watchdog: increment consecutive write-failure counter and
            # trigger force-reconnect if threshold exceeded.
            self._note_write_failure()
            return {}

    def cancel_order(self, order_id: str = None, symbol: str = None, order_link_id: str = None) -> bool:
        """Cancel order via WebSocket Trade API with latency tracking.

        Can cancel by either orderId OR orderLinkId (professional approach).
        orderLinkId is preferred as it's generated locally and doesn't require API latency.
        """
        if not self.connected or not self.ws:
            return False

        if not order_id and not order_link_id:
            if self.logger:
                self.logger.error("[WS-CANCEL] Need either order_id or order_link_id")
            return False

        t_start = time.perf_counter()

        try:
            bybit_symbol = symbol.replace("/", "").replace(":USDT", "")
            req_id = str(uuid.uuid4())[:8]
            timestamp = str(int(time.time() * 1000))

            cancel_args = {
                "category": "linear",
                "symbol": bybit_symbol,
            }

            # Prefer orderLinkId if provided (more reliable - we generate it)
            if order_link_id:
                cancel_args["orderLinkId"] = order_link_id
            else:
                cancel_args["orderId"] = order_id

            msg = {
                "reqId": req_id,
                "op": "order.cancel",
                "header": {
                    "X-BAPI-TIMESTAMP": timestamp,
                    "X-BAPI-RECV-WINDOW": "5000",
                    "Referer": "Nu000450"  # Broker ID for affiliate tracking
                },
                "args": [cancel_args]
            }

            self.ws.send(json.dumps(msg))
            response = self._wait_for_response(req_id, timeout=3.0)

            t_end = time.perf_counter()
            latency_ms = (t_end - t_start) * 1000

            id_str = order_link_id or order_id
            ret_code = response.get("retCode") if response else -1

            if ret_code == 0:
                # Successful cancel
                if self.logger:
                    self.logger.info(f"[WS] Cancelled {id_str} for {symbol} latency={latency_ms:.1f}ms")
                self._consecutive_write_failures = 0  # reset watchdog
                return True
            elif ret_code == 110001:
                # Order already filled/cancelled - treat as success (order is gone)
                if self.logger:
                    self.logger.info(f"[WS] Order {id_str} already gone (filled/cancelled) latency={latency_ms:.1f}ms")
                self._consecutive_write_failures = 0  # reset watchdog
                return True  # Return True so caller knows to unregister
            else:
                if self.logger:
                    self.logger.warning(f"[WS-CANCEL-FAIL] {id_str} response={response} latency={latency_ms:.1f}ms")
                return False

        except Exception as e:
            t_end = time.perf_counter()
            latency_ms = (t_end - t_start) * 1000
            id_str = order_link_id or order_id
            if self.logger:
                self.logger.error(f"[WS-CANCEL-ERROR] {id_str} error={e} latency={latency_ms:.1f}ms")
            # Watchdog: cancel timeouts also indicate dead pipe
            self._note_write_failure()
            return False

    def amend_order(
        self,
        symbol: str,
        order_id: str = None,
        order_link_id: str = None,
        qty: float = None,
        price: float = None,
    ) -> bool:
        """
        Amend an existing order's price/qty via WebSocket Trade API.

        ZERO NAKED WINDOW: Changes price/qty in-place without cancel+replace.
        The order stays on the book the entire time — no gap where we're unquoted.

        Returns True on success.
        """
        if not self.connected or not self.ws:
            return False

        if not order_id and not order_link_id:
            if self.logger:
                self.logger.error("[WS-AMEND] Need either order_id or order_link_id")
            return False

        if qty is None and price is None:
            if self.logger:
                self.logger.error("[WS-AMEND] Need at least qty or price to amend")
            return False

        t_start = time.perf_counter()

        try:
            bybit_symbol = symbol.replace("/", "").replace(":USDT", "")
            req_id = str(uuid.uuid4())[:8]
            timestamp = str(int(time.time() * 1000))

            amend_args = {
                "category": "linear",
                "symbol": bybit_symbol,
            }

            if order_link_id:
                amend_args["orderLinkId"] = order_link_id
            else:
                amend_args["orderId"] = order_id

            if qty is not None:
                rounded_qty = round(qty)
                if rounded_qty < 1:
                    rounded_qty = 1
                amend_args["qty"] = str(int(rounded_qty))

            if price is not None:
                amend_args["price"] = self._snap_price(symbol, price)

            msg = {
                "reqId": req_id,
                "op": "order.amend",
                "header": {
                    "X-BAPI-TIMESTAMP": timestamp,
                    "X-BAPI-RECV-WINDOW": "5000",
                    "Referer": "Nu000450"
                },
                "args": [amend_args]
            }

            self.ws.send(json.dumps(msg))
            response = self._wait_for_response(req_id, timeout=3.0)

            t_end = time.perf_counter()
            latency_ms = (t_end - t_start) * 1000

            id_str = order_link_id or order_id
            ret_code = response.get("retCode") if response else -1

            if ret_code == 0:
                if self.logger:
                    self.logger.info(
                        f"[WS-AMEND] {id_str} → price={price} qty={qty} "
                        f"latency={latency_ms:.1f}ms"
                    )
                return True
            elif ret_code == 110001:
                # Order already filled/cancelled — can't amend
                if self.logger:
                    self.logger.info(f"[WS-AMEND] {id_str} already gone, latency={latency_ms:.1f}ms")
                return False
            else:
                if self.logger:
                    self.logger.warning(
                        f"[WS-AMEND-FAIL] {id_str} ret={ret_code} "
                        f"response={response} latency={latency_ms:.1f}ms"
                    )
                return False

        except Exception as e:
            t_end = time.perf_counter()
            latency_ms = (t_end - t_start) * 1000
            id_str = order_link_id or order_id
            if self.logger:
                self.logger.error(f"[WS-AMEND-ERROR] {id_str} error={e} latency={latency_ms:.1f}ms")
            return False

    def place_orders_batch(
        self,
        orders: List[Dict],
        symbol: str,
        position_idx: int = 0,
        skip_cancel_ids: Optional[List[str]] = None,
    ) -> List[Dict]:
        """
        Place multiple orders via WebSocket batch API (up to 20 orders in one message).

        MASSIVE LATENCY IMPROVEMENT:
        - Sequential: 8 orders × 190ms = 1.5 seconds
        - Batch: 1 message × 190ms = 190ms (8x faster!)

        Args:
            orders: List of order dicts with keys:
                - side: 'buy' or 'sell'
                - amount: float
                - price: float
                - order_link_id: str (optional)
                - reduce_only: bool (optional)
                - post_only: bool (optional, default True)
            symbol: Trading symbol (e.g., "ASTER/USDT:USDT")
            position_idx: Position index for hedge mode (0=one-way, 1=buy-side, 2=sell-side)

        Returns:
            List of order result dicts (success/failure for each)
        """
        if not self.connected or not self.ws:
            return []

        if not orders:
            return []

        # Bybit limit: 20 orders per batch for linear
        if len(orders) > 20:
            if self.logger:
                self.logger.warning(f"[WS-BATCH] Truncating {len(orders)} orders to 20 (Bybit limit)")
            orders = orders[:20]

        t_start = time.perf_counter()

        try:
            bybit_symbol = symbol.replace("/", "").replace(":USDT", "")
            req_id = str(uuid.uuid4())[:8]
            timestamp = str(int(time.time() * 1000))

            # Build request array for batch
            request_list = []
            for order in orders:
                side = order.get('side', 'buy')
                amount = order.get('amount', 0)
                price = order.get('price')
                order_link_id = order.get('order_link_id')
                reduce_only = order.get('reduce_only', False)
                post_only = order.get('post_only', True)

                # Snap qty to the symbol's actual lot step (LINK=0.1, XRP=1.0,
                # BTC=0.001, etc.). Same fix as in send_order above — was
                # previously hardcoded to round-to-integer.
                snapped_qty = self._snap_qty(symbol, amount)
                qty_str = self._format_qty(symbol, snapped_qty)

                # Determine positionIdx for hedge mode
                # In hedge mode: 1=buy side (long), 2=sell side (short)
                # In one-way mode: 0
                if position_idx == 0:
                    # Auto-detect for hedge mode based on side
                    order_position_idx = 1 if side.lower() == 'buy' else 2
                else:
                    order_position_idx = position_idx

                order_args = {
                    "symbol": bybit_symbol,
                    "side": side.capitalize(),
                    "orderType": "Limit",
                    "qty": qty_str,
                    "price": self._snap_price(symbol, price),
                    "positionIdx": order_position_idx,
                    "timeInForce": "PostOnly" if post_only else "GTC",
                }

                if order_link_id:
                    order_args["orderLinkId"] = order_link_id

                if reduce_only:
                    order_args["reduceOnly"] = True

                request_list.append(order_args)

            msg = {
                "reqId": req_id,
                "op": "order.create-batch",
                "header": {
                    "X-BAPI-TIMESTAMP": timestamp,
                    "X-BAPI-RECV-WINDOW": "5000",
                    "Referer": "Nu000450"
                },
                "args": [{
                    "category": "linear",
                    "request": request_list
                }]
            }

            if self.logger:
                self.logger.info(f"[WS-BATCH-SEND] {len(orders)} orders for {symbol}")

            self.ws.send(json.dumps(msg))
            response = self._wait_for_response(req_id, timeout=5.0)

            t_end = time.perf_counter()
            latency_ms = (t_end - t_start) * 1000


            results = []
            if response and response.get("retCode") == 0:
                # Parse batch response
                data_list = response.get("data", {}).get("list", [])
                ext_info = response.get("retExtInfo", {}).get("list", [])

                for i, item in enumerate(data_list):
                    order_id = item.get("orderId", "")
                    order_link_id = item.get("orderLinkId", "")

                    # Check individual order status from retExtInfo
                    item_code = ext_info[i].get("code", 0) if i < len(ext_info) else 0
                    item_msg = ext_info[i].get("msg", "") if i < len(ext_info) else ""

                    if item_code == 0:
                        results.append({
                            'id': order_id,
                            'clientOrderId': order_link_id,
                            'symbol': symbol,
                            'status': 'open',
                            'success': True
                        })
                    else:
                        results.append({
                            'clientOrderId': order_link_id,
                            'symbol': symbol,
                            'status': 'failed',
                            'success': False,
                            'error_code': item_code,
                            'error_msg': item_msg
                        })

                success_count = sum(1 for r in results if r.get('success'))
                if self.logger:
                    self.logger.info(f"[WS-BATCH-OK] {success_count}/{len(orders)} orders placed in {latency_ms:.1f}ms")
            else:
                ret_code = response.get("retCode") if response else "NO_RESPONSE"
                ret_msg = response.get("retMsg", "timeout") if response else "timeout"
                if self.logger:
                    self.logger.warning(f"[WS-BATCH-FAIL] code={ret_code} msg={ret_msg} latency={latency_ms:.1f}ms")

            return results

        except Exception as e:
            t_end = time.perf_counter()
            latency_ms = (t_end - t_start) * 1000
            if self.logger:
                self.logger.error(f"[WS-BATCH-ERROR] {e} latency={latency_ms:.1f}ms")
            return []

    def cancel_orders_batch(
        self,
        orders: List[Dict],
        symbol: str
    ) -> List[bool]:
        """
        Cancel multiple orders via WebSocket batch API (up to 20 orders in one message).

        Args:
            orders: List of order dicts with keys:
                - order_id: str (optional)
                - order_link_id: str (optional, preferred)
            symbol: Trading symbol

        Returns:
            List of bools indicating success/failure for each cancel
        """
        if not self.connected or not self.ws:
            return [False] * len(orders)

        if not orders:
            return []

        # Bybit limit: 20 orders per batch
        if len(orders) > 20:
            if self.logger:
                self.logger.warning(f"[WS-BATCH-CANCEL] Truncating {len(orders)} cancels to 20")
            orders = orders[:20]

        t_start = time.perf_counter()

        try:
            bybit_symbol = symbol.replace("/", "").replace(":USDT", "")
            req_id = str(uuid.uuid4())[:8]
            timestamp = str(int(time.time() * 1000))

            # Build cancel request array
            request_list = []
            for order in orders:
                cancel_args = {"symbol": bybit_symbol}

                # Prefer orderLinkId if available
                if order.get('order_link_id'):
                    cancel_args["orderLinkId"] = order['order_link_id']
                elif order.get('order_id'):
                    cancel_args["orderId"] = order['order_id']
                else:
                    continue  # Skip if no identifier

                request_list.append(cancel_args)

            if not request_list:
                return []

            msg = {
                "reqId": req_id,
                "op": "order.cancel-batch",
                "header": {
                    "X-BAPI-TIMESTAMP": timestamp,
                    "X-BAPI-RECV-WINDOW": "5000",
                    "Referer": "Nu000450"
                },
                "args": [{
                    "category": "linear",
                    "request": request_list
                }]
            }

            if self.logger:
                self.logger.info(f"[WS-BATCH-CANCEL-SEND] {len(request_list)} orders for {symbol}")

            self.ws.send(json.dumps(msg))
            response = self._wait_for_response(req_id, timeout=5.0)

            t_end = time.perf_counter()
            latency_ms = (t_end - t_start) * 1000

            results = [False] * len(request_list)
            if response:
                ret_code = response.get("retCode", -1)
                if ret_code == 0:
                    # Parse individual results
                    ext_info = response.get("retExtInfo", {}).get("list", [])
                    for i in range(len(request_list)):
                        if i < len(ext_info):
                            item_code = ext_info[i].get("code", 0)
                            # 0 = success, 110001 = already cancelled/filled (also success)
                            results[i] = item_code in (0, 110001)
                        else:
                            results[i] = True  # Assume success if no error info

                    success_count = sum(results)
                    if self.logger:
                        self.logger.info(f"[WS-BATCH-CANCEL-OK] {success_count}/{len(request_list)} cancelled in {latency_ms:.1f}ms")
                else:
                    if self.logger:
                        self.logger.warning(f"[WS-BATCH-CANCEL-FAIL] code={ret_code} latency={latency_ms:.1f}ms")
            else:
                if self.logger:
                    self.logger.warning(f"[WS-BATCH-CANCEL-TIMEOUT] latency={latency_ms:.1f}ms")

            return results

        except Exception as e:
            t_end = time.perf_counter()
            latency_ms = (t_end - t_start) * 1000
            if self.logger:
                self.logger.error(f"[WS-BATCH-CANCEL-ERROR] {e} latency={latency_ms:.1f}ms")
            return [False] * len(orders)


class WebSocketDataManager:
    """Real-time market data via Bybit WebSocket v5.

    Public: wss://stream.bybit.com/v5/public/linear
      - orderbook.50.{SYMBOL} → 50-level orderbook (snapshot + delta)
      - publicTrade.{SYMBOL}  → trade feed
      - tickers.{SYMBOL}      → last price, 24h vol, funding

    Private: wss://stream.bybit.com/v5/private
      - position              → real-time position updates
      - execution             → fill notifications

    All data cached in memory. Strategy reads from cache (0ms) instead of REST (~250ms).
    Falls back to REST if WS disconnects.
    """

    PUBLIC_URL = "wss://stream.bybit.com/v5/public/linear"
    PUBLIC_URL_TESTNET = "wss://stream-testnet.bybit.com/v5/public/linear"
    PRIVATE_URL = "wss://stream.bybit.com/v5/private"
    PRIVATE_URL_TESTNET = "wss://stream-testnet.bybit.com/v5/private"

    def __init__(self, api_key: str, api_secret: str, testnet: bool = False, logger=None,
                 record_l2: bool = False, l2_base: str = "data/l2",
                 l2_min_interval: float = 0.5):
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        self.logger = logger

        # --- L2 recorder: append capped-rate top-20 book snapshots to
        # data/l2/{symbol}.jsonl so a future book-replay backtester is possible.
        # Fully OFF by default (record_l2=False) → zero impact on the connector.
        self.record_l2 = bool(record_l2)
        self.l2_base = l2_base
        self.l2_min_interval = float(l2_min_interval)  # >=0.5s → <=2 Hz cap
        self._l2_last_write: Dict[str, float] = {}

        # Cached data (thread-safe via GIL for simple dict/deque assignment)
        self._orderbooks: Dict[str, Dict] = {}       # symbol -> {bids, asks, ts}
        self._tickers: Dict[str, Dict] = {}           # symbol -> {last, ...}
        self._positions: Dict[str, Dict] = {}          # symbol -> {long: {...}, short: {...}}
        self._recent_trades: Dict[str, list] = {}      # symbol -> list of {price, qty, side, timestamp}
        self._recent_executions: Dict[str, list] = {}  # symbol -> private fills

        self._position_seeded: set = set()  # symbols seeded from REST

        self._public_ws = None
        self._private_ws = None
        self._running = False
        self._connected_public = False
        self._connected_private = False
        self._subscribed_symbols: List[str] = []

        # Position defaults template
        self._default_position_side = {
            "qty": 0.0, "price": 0.0, "realised": 0, "cum_realised": 0,
            "upnl": 0, "upnl_pct": 0, "liq_price": 0, "entry_price": 0,
        }

    def subscribe(self, bybit_symbol: str):
        """Subscribe to data streams for a symbol. Can be called after connect()."""
        if bybit_symbol in self._subscribed_symbols:
            return

        self._subscribed_symbols.append(bybit_symbol)

        # Initialize caches
        self._orderbooks[bybit_symbol] = {'bids': [], 'asks': [], 'timestamp': 0}
        self._tickers[bybit_symbol] = {'last': 0.0}
        self._recent_trades[bybit_symbol] = []
        if bybit_symbol not in self._positions:
            self._positions[bybit_symbol] = {
                'long': dict(self._default_position_side),
                'short': dict(self._default_position_side),
            }

        # Subscribe on public WS
        if self._public_ws and self._connected_public:
            try:
                sub_msg = {
                    "op": "subscribe",
                    "args": [
                        f"orderbook.50.{bybit_symbol}",
                        f"publicTrade.{bybit_symbol}",
                        f"tickers.{bybit_symbol}",
                    ]
                }
                self._public_ws.send(json.dumps(sub_msg))
                if self.logger:
                    self.logger.info(f"[WS-DATA] Subscribed public: {bybit_symbol}")
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"[WS-DATA] Public subscribe failed: {e}")

    def connect(self) -> bool:
        """Connect public + private WebSocket streams in background threads."""
        if not WEBSOCKET_AVAILABLE:
            if self.logger:
                self.logger.warning("[WS-DATA] websocket-client not installed")
            return False

        self._running = True

        # Public WS thread
        pub_thread = threading.Thread(target=self._run_public, daemon=True)
        pub_thread.start()

        # Private WS thread
        priv_thread = threading.Thread(target=self._run_private, daemon=True)
        priv_thread.start()

        # Wait briefly for connections
        for _ in range(30):  # 3 seconds max
            if self._connected_public and self._connected_private:
                if self.logger:
                    self.logger.info("[WS-DATA] Both public and private streams connected")
                return True
            time.sleep(0.1)

        if self.logger:
            self.logger.warning(
                f"[WS-DATA] Partial connect: public={self._connected_public} "
                f"private={self._connected_private}"
            )
        return self._connected_public  # Public is essential, private is nice-to-have

    def _run_public(self):
        """Public WS connection loop with reconnection."""
        backoff = 1.0
        while self._running:
            try:
                url = self.PUBLIC_URL_TESTNET if self.testnet else self.PUBLIC_URL
                ws = websocket.create_connection(url, timeout=30)
                self._public_ws = ws
                self._connected_public = True
                backoff = 1.0

                if self.logger:
                    self.logger.info(f"[WS-DATA] Public connected: {url}")

                # Subscribe to all current symbols
                for sym in self._subscribed_symbols:
                    sub_msg = {
                        "op": "subscribe",
                        "args": [
                            f"orderbook.50.{sym}",
                            f"publicTrade.{sym}",
                            f"tickers.{sym}",
                        ]
                    }
                    ws.send(json.dumps(sub_msg))

                # Receive loop
                last_ping = time.time()
                while self._running:
                    try:
                        msg = ws.recv()
                        if not msg:
                            continue

                        # Heartbeat
                        now = time.time()
                        if now - last_ping > 18:
                            ws.send(json.dumps({"op": "ping"}))
                            last_ping = now

                        data = json.loads(msg)

                        # Route by topic
                        topic = data.get('topic', '')
                        if topic.startswith('orderbook.'):
                            self._on_orderbook(data)
                        elif topic.startswith('publicTrade.'):
                            self._on_public_trade(data)
                        elif topic.startswith('tickers.'):
                            self._on_ticker(data)
                        # pong and subscribe confirmations — ignore

                    except websocket.WebSocketTimeoutException:
                        # Send ping on timeout
                        try:
                            ws.send(json.dumps({"op": "ping"}))
                            last_ping = time.time()
                        except Exception:
                            break
                    except Exception as e:
                        if self._running and self.logger:
                            self.logger.warning(f"[WS-DATA] Public recv error: {e}")
                        break

            except Exception as e:
                if self._running and self.logger:
                    self.logger.warning(f"[WS-DATA] Public connect error: {e}, retry in {backoff}s")

            self._connected_public = False
            self._public_ws = None
            if self._running:
                time.sleep(backoff)
                backoff = min(30.0, backoff * 2)

    def _run_private(self):
        """Private WS connection loop with auth and reconnection."""
        backoff = 1.0
        while self._running:
            try:
                url = self.PRIVATE_URL_TESTNET if self.testnet else self.PRIVATE_URL
                ws = websocket.create_connection(url, timeout=30)

                # Authenticate
                expires = int(time.time() * 1000) + 10000
                param_str = f"GET/realtime{expires}"
                signature = hmac.new(
                    self.api_secret.encode('utf-8'),
                    param_str.encode('utf-8'),
                    hashlib.sha256
                ).hexdigest()
                auth_msg = {"op": "auth", "args": [self.api_key, expires, signature]}
                ws.send(json.dumps(auth_msg))
                auth_resp = json.loads(ws.recv())

                if not (auth_resp.get("success") or auth_resp.get("retCode") == 0):
                    if self.logger:
                        self.logger.error(f"[WS-DATA] Private auth failed: {auth_resp}")
                    ws.close()
                    time.sleep(backoff)
                    backoff = min(30.0, backoff * 2)
                    continue

                # Subscribe to position + execution
                ws.send(json.dumps({
                    "op": "subscribe",
                    "args": ["position", "execution"]
                }))

                self._private_ws = ws
                self._connected_private = True
                backoff = 1.0

                if self.logger:
                    self.logger.info(f"[WS-DATA] Private connected and authenticated")

                # Receive loop
                last_ping = time.time()
                while self._running:
                    try:
                        msg = ws.recv()
                        if not msg:
                            continue

                        now = time.time()
                        if now - last_ping > 18:
                            ws.send(json.dumps({"op": "ping"}))
                            last_ping = now

                        data = json.loads(msg)
                        topic = data.get('topic', '')

                        if topic == 'position':
                            self._on_position(data)
                        elif topic == 'execution':
                            self._on_execution(data)

                    except websocket.WebSocketTimeoutException:
                        try:
                            ws.send(json.dumps({"op": "ping"}))
                            last_ping = time.time()
                        except Exception:
                            break
                    except Exception as e:
                        if self._running and self.logger:
                            self.logger.warning(f"[WS-DATA] Private recv error: {e}")
                        break

            except Exception as e:
                if self._running and self.logger:
                    self.logger.warning(f"[WS-DATA] Private connect error: {e}, retry in {backoff}s")

            self._connected_private = False
            self._private_ws = None
            if self._running:
                time.sleep(backoff)
                backoff = min(30.0, backoff * 2)

    # === Message handlers ===

    def _on_orderbook(self, data: Dict):
        """Handle orderbook snapshot/delta updates."""
        topic = data.get('topic', '')
        parts = topic.split('.')
        if len(parts) < 3:
            return
        symbol = parts[2]  # e.g. DOGEUSDT

        book_data = data.get('data', {})
        msg_type = data.get('type', '')

        if msg_type == 'snapshot':
            # Full replace
            bids = [[float(p), float(q)] for p, q in book_data.get('b', [])]
            asks = [[float(p), float(q)] for p, q in book_data.get('a', [])]
            self._orderbooks[symbol] = {
                'bids': bids,
                'asks': asks,
                'timestamp': time.time(),
            }
        elif msg_type == 'delta':
            # Apply deltas to existing book
            ob = self._orderbooks.get(symbol)
            if not ob or not ob.get('bids'):
                return  # Wait for snapshot

            # Apply bid deltas
            for price_str, qty_str in book_data.get('b', []):
                price, qty = float(price_str), float(qty_str)
                if qty == 0:
                    ob['bids'] = [l for l in ob['bids'] if l[0] != price]
                else:
                    updated = False
                    for i, level in enumerate(ob['bids']):
                        if level[0] == price:
                            ob['bids'][i] = [price, qty]
                            updated = True
                            break
                    if not updated:
                        ob['bids'].append([price, qty])
                    ob['bids'].sort(key=lambda x: x[0], reverse=True)

            # Apply ask deltas
            for price_str, qty_str in book_data.get('a', []):
                price, qty = float(price_str), float(qty_str)
                if qty == 0:
                    ob['asks'] = [l for l in ob['asks'] if l[0] != price]
                else:
                    updated = False
                    for i, level in enumerate(ob['asks']):
                        if level[0] == price:
                            ob['asks'][i] = [price, qty]
                            updated = True
                            break
                    if not updated:
                        ob['asks'].append([price, qty])
                    ob['asks'].sort(key=lambda x: x[0])

            ob['timestamp'] = time.time()

        # L2 recorder — guarded, capped-rate, zero work when off.
        if self.record_l2:
            self._record_l2_snapshot(symbol)

    def _record_l2_snapshot(self, symbol: str) -> None:
        """Append a capped-rate top-20 book snapshot to data/l2/{symbol}.jsonl.

        Guarded by ``record_l2`` (caller) + a per-symbol min-interval so the
        write rate is capped (default 0.5s → <=2 Hz). Best-effort: any failure
        is swallowed so the WS callback never breaks the feed.
        """
        try:
            from pathlib import Path
            now = time.time()
            last = self._l2_last_write.get(symbol, 0.0)
            if (now - last) < self.l2_min_interval:
                return
            ob = self._orderbooks.get(symbol)
            if not ob:
                return
            rec = {
                "ts": now,
                "bids": ob.get("bids", [])[:20],
                "asks": ob.get("asks", [])[:20],
            }
            base = Path(self.l2_base)
            base.mkdir(parents=True, exist_ok=True)
            with open(base / f"{symbol}.jsonl", "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, separators=(",", ":")) + "\n")
            self._l2_last_write[symbol] = now
        except Exception as e:
            if self.logger:
                self.logger.warning(f"L2 recorder write failed for {symbol}: {e}")

    def _on_ticker(self, data: Dict):
        """Handle ticker updates."""
        topic = data.get('topic', '')
        parts = topic.split('.')
        if len(parts) < 2:
            return
        symbol = parts[1]

        tick_data = data.get('data', {})
        last_price = tick_data.get('lastPrice')
        if last_price:
            existing = self._tickers.get(symbol, {})
            existing['last'] = float(last_price)
            if tick_data.get('markPrice'):
                existing['mark'] = float(tick_data['markPrice'])
            if tick_data.get('highPrice24h'):
                existing['high24h'] = float(tick_data['highPrice24h'])
            if tick_data.get('lowPrice24h'):
                existing['low24h'] = float(tick_data['lowPrice24h'])
            existing['timestamp'] = time.time()
            self._tickers[symbol] = existing

    def _on_public_trade(self, data: Dict):
        """Handle public trade updates."""
        topic = data.get('topic', '')
        parts = topic.split('.')
        if len(parts) < 2:
            return
        symbol = parts[1]

        trades = data.get('data', [])
        if not trades:
            return

        trade_list = self._recent_trades.get(symbol, [])
        for t in trades:
            ts_raw = t.get('T', 0)
            trade_list.append({
                'price': float(t.get('p', 0)),
                'qty': float(t.get('v', 0)),
                'side': t.get('S', 'Buy').lower(),
                'timestamp': ts_raw / 1000.0 if ts_raw > 1e12 else float(ts_raw),
            })

        # Keep last 500 trades
        if len(trade_list) > 500:
            trade_list = trade_list[-500:]
        self._recent_trades[symbol] = trade_list

    def _on_position(self, data: Dict):
        """Handle private position updates."""
        positions = data.get('data', [])
        for pos in positions:
            symbol = pos.get('symbol', '')
            if not symbol:
                continue

            pos_idx = pos.get('positionIdx', 0)
            if pos_idx == 1:
                side_key = 'long'
            elif pos_idx == 2:
                side_key = 'short'
            else:
                continue

            # Initialize if needed
            if symbol not in self._positions:
                self._positions[symbol] = {
                    'long': dict(self._default_position_side),
                    'short': dict(self._default_position_side),
                }

            upnl = float(pos.get('unrealisedPnl', 0) or 0)
            pos_im = float(pos.get('positionIM', 0) or 0)
            upnl_pct = round((upnl / pos_im * 100) if pos_im > 0 else 0, 4)

            self._positions[symbol][side_key] = {
                'qty': float(pos.get('size', 0)),
                'price': float(pos.get('entryPrice', 0) or 0),
                'realised': round(upnl, 4),
                'cum_realised': round(float(pos.get('cumRealisedPnl', 0) or 0), 4),
                'upnl': round(upnl, 4),
                'upnl_pct': upnl_pct,
                'liq_price': float(pos.get('liqPrice', 0) or 0),
                'entry_price': float(pos.get('entryPrice', 0) or 0),
            }

    def _on_execution(self, data: Dict):
        """Handle execution (fill) notifications."""
        for exec_data in data.get('data', []):
            symbol = exec_data.get('symbol', '')
            if symbol:
                exec_list = self._recent_executions.get(symbol, [])
                ts_raw = exec_data.get('execTime') or exec_data.get('T') or 0
                try:
                    ts = float(ts_raw) / 1000.0 if float(ts_raw) > 1e12 else float(ts_raw)
                except (TypeError, ValueError):
                    ts = time.time()
                try:
                    qty = float(exec_data.get('execQty') or 0)
                except (TypeError, ValueError):
                    qty = 0.0
                try:
                    price = float(exec_data.get('execPrice') or 0)
                except (TypeError, ValueError):
                    price = 0.0
                side = str(exec_data.get('side') or '').lower()
                exec_list.append({
                    'symbol': symbol,
                    'side': side,
                    'qty': qty,
                    'price': price,
                    'exec_time': ts,
                    'order_id': exec_data.get('orderId'),
                    'order_link_id': exec_data.get('orderLinkId'),
                })
                if len(exec_list) > 500:
                    exec_list = exec_list[-500:]
                self._recent_executions[symbol] = exec_list
            if self.logger:
                self.logger.info(
                    f"[WS-EXEC] {exec_data.get('symbol')} {exec_data.get('side')} "
                    f"qty={exec_data.get('execQty')} @ {exec_data.get('execPrice')} "
                    f"fee={exec_data.get('execFee')}"
                )

    # === Cache getters ===

    def get_orderbook(self, symbol: str) -> Optional[Dict]:
        """Get cached orderbook. Returns None if no data or stale >5s."""
        ob = self._orderbooks.get(symbol)
        if ob and ob.get('bids') and ob.get('asks'):
            if time.time() - ob.get('timestamp', 0) < 5.0:
                return ob
        return None

    def get_ticker(self, symbol: str) -> Optional[Dict]:
        """Get cached ticker. Returns None if no data or stale >5s."""
        tick = self._tickers.get(symbol)
        if tick and tick.get('last', 0) > 0:
            if time.time() - tick.get('timestamp', 0) < 5.0:
                return tick
        return None

    def get_positions(self, symbol: str) -> Optional[Dict]:
        """Get cached positions. Returns None if never received."""
        return self._positions.get(symbol)

    def get_recent_trades(self, symbol: str) -> Optional[list]:
        """Get cached recent trades. Returns None if empty."""
        trades = self._recent_trades.get(symbol)
        if trades:
            return trades
        return None

    def get_recent_executions(self, symbol: str) -> Optional[list]:
        """Get cached private executions, newest first. Returns None if empty."""
        executions = self._recent_executions.get(symbol)
        if executions:
            return sorted(executions, key=lambda e: e.get('exec_time', 0), reverse=True)
        return None

    def disconnect(self):
        """Clean shutdown of both WS connections."""
        self._running = False
        for ws in [self._public_ws, self._private_ws]:
            if ws:
                try:
                    ws.close()
                except Exception:
                    pass
        self._public_ws = None
        self._private_ws = None
        self._connected_public = False
        self._connected_private = False
        if self.logger:
            self.logger.info("[WS-DATA] Disconnected")


class BybitExchange(BaseExchange):
    """Bybit exchange implementation - Institutional Grade"""

    def __init__(self, config: Dict, logger=None):
        super().__init__(config, logger)
        self.rate_limit_delay = 0.01  # 10ms between requests (most data via WS now)

        # WebSocket order support (faster than REST)
        self.websocket_orders_enabled = config.get('websocket_orders', False)
        self.websocket_data_enabled = config.get('websocket_data', False)
        self.ws_order_manager = None

        # === INSTITUTIONAL ORDER TRACKING ===
        # Order state machine: tracks all orders by clientOrderId (orderLinkId)
        self._order_states = {}  # orderLinkId -> {state, sent_at, order_id, symbol, side, amount, price, latency_ms}
        self._order_lock = threading.Lock()

        # === CENTRALIZED ORDER REGISTRY ===
        # Track all active orders by orderLinkId for universal order deduplication
        # Eliminates need for per-strategy registration - all orders auto-registered
        self._order_registry: Dict[str, Dict] = {}  # orderLinkId -> {side, price, qty, order_id, placed_at}

        # WebSocket data streams (real-time market data, 0ms reads)
        self._ws_data: Optional[WebSocketDataManager] = None

        # Latency metrics for monitoring
        self._latency_samples = []  # Recent latencies for p50/p95/p99 calculation
        self._max_latency_samples = 100

        if self.websocket_orders_enabled and not WEBSOCKET_AVAILABLE:
            self.logger.warning("websocket_orders enabled but websocket-client not installed - falling back to REST")
            self.websocket_orders_enabled = False
        if self.websocket_data_enabled and not WEBSOCKET_AVAILABLE:
            self.logger.warning("websocket_data enabled but websocket-client not installed - falling back to REST")
            self.websocket_data_enabled = False

    def _snap_to_tick(self, symbol: str, price: float) -> float:
        """Snap price to tick size for a symbol. Uses cached CCXT market data."""
        try:
            market = self.exchange.market(symbol)
            tick = market['precision']['price']
            if isinstance(tick, float) and tick > 0:
                return round(round(price / tick) * tick, 10)
            elif isinstance(tick, int):
                return round(price, tick)
        except Exception:
            pass
        return price

    def get_qty_step(self, symbol: str) -> Optional[float]:
        """Public: the symbol's qty (lot) step from cached CCXT market data, or
        None if it can't be determined. Used by the strategy's reduce-only flat
        guard to decide 'below step -> truncates to zero -> skip' (the Bybit
        110017 prevention). Read-only — never places/cancels."""
        try:
            market = self.exchange.market(symbol)
            step = market['precision']['amount']
            if isinstance(step, (int, float)) and float(step) > 0:
                return float(step)
        except Exception:
            pass
        return None

    def _snap_qty(self, symbol: str, qty: float) -> float:
        """Snap qty to step size for a symbol. Uses cached CCXT market data."""
        try:
            market = self.exchange.market(symbol)
            step = market['precision']['amount']
            if isinstance(step, float) and step > 0:
                if step >= 1.0:
                    return max(1.0, float(round(qty)))
                return round(round(qty / step) * step, 8)
            elif isinstance(step, int):
                return round(qty, step)
        except Exception:
            pass
        return qty

    def _generate_order_link_id(self, symbol: str, side: str) -> str:
        """
        Generate unique client order ID for idempotency.
        Format: {side[0]}_{symbol}_{timestamp}_{uuid8}
        Example: B_ASTERUSDT_20260130143052_a3f2b1c4
        """
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        symbol_clean = symbol.replace('/', '').replace(':', '')
        return f"{side[0].upper()}_{symbol_clean}_{timestamp}_{unique_id}"

    def _record_order_state(self, order_link_id: str, state: OrderState, **kwargs):
        """Record order state transition for tracking"""
        with self._order_lock:
            if order_link_id not in self._order_states:
                self._order_states[order_link_id] = {
                    'created_at': time.time(),
                    'transitions': []
                }

            self._order_states[order_link_id]['state'] = state.value
            self._order_states[order_link_id]['updated_at'] = time.time()
            self._order_states[order_link_id].update(kwargs)
            self._order_states[order_link_id]['transitions'].append({
                'state': state.value,
                'timestamp': time.time()
            })

            # Log state transition
            if self.logger:
                self.logger.info(f"[ORDER-STATE] {order_link_id} → {state.value} | {kwargs}")

            # Cleanup old orders (keep last 1000)
            if len(self._order_states) > 1000:
                oldest_keys = sorted(self._order_states.keys(),
                                    key=lambda k: self._order_states[k].get('created_at', 0))[:100]
                for k in oldest_keys:
                    del self._order_states[k]

    def _record_latency(self, latency_ms: float):
        """Record latency sample for metrics"""
        with self._order_lock:
            self._latency_samples.append(latency_ms)
            if len(self._latency_samples) > self._max_latency_samples:
                self._latency_samples.pop(0)

    def get_latency_stats(self) -> Dict:
        """Get latency percentiles (p50, p95, p99)"""
        with self._order_lock:
            if not self._latency_samples:
                return {'p50': 0, 'p95': 0, 'p99': 0, 'count': 0}

            sorted_samples = sorted(self._latency_samples)
            n = len(sorted_samples)
            return {
                'p50': sorted_samples[int(n * 0.50)] if n > 0 else 0,
                'p95': sorted_samples[int(n * 0.95)] if n > 0 else 0,
                'p99': sorted_samples[int(n * 0.99)] if n > 0 else 0,
                'count': n
            }

    def get_order_state(self, order_link_id: str) -> Optional[Dict]:
        """Get current state of an order by clientOrderId"""
        with self._order_lock:
            return self._order_states.get(order_link_id)

    def verify_fill(self, order_link_id: str, symbol: str) -> Dict:
        """
        Verify order fill status from exchange.

        INSTITUTIONAL: Confirms order was actually filled by querying exchange.
        Updates internal state tracking with verified status.

        Returns:
            Dict with keys: filled (bool), fill_price, fill_qty, fees, status
        """
        try:
            t_start = time.perf_counter()
            bybit_symbol = symbol.replace("/", "").replace(":USDT", "")

            # Query order by orderLinkId
            self._rate_limit()
            response = self.exchange.fetch_orders(
                symbol=symbol,
                params={'orderLinkId': order_link_id}
            )

            t_end = time.perf_counter()
            latency_ms = (t_end - t_start) * 1000

            if not response:
                self.logger.warning(f"[VERIFY] No order found for linkId={order_link_id}")
                return {'filled': False, 'status': 'not_found', 'latency_ms': round(latency_ms, 1)}

            order = response[0] if isinstance(response, list) else response

            status = order.get('status', 'unknown')
            filled_qty = float(order.get('filled', 0))
            avg_price = float(order.get('average', 0)) if order.get('average') else None
            fee_info = order.get('fee', {})

            # Map CCXT status to our OrderState
            if status == 'closed' and filled_qty > 0:
                self._record_order_state(
                    order_link_id,
                    OrderState.FILLED,
                    fill_qty=filled_qty,
                    fill_price=avg_price
                )
                is_filled = True
            elif status == 'canceled':
                self._record_order_state(order_link_id, OrderState.CANCELLED)
                is_filled = False
            elif filled_qty > 0 and filled_qty < float(order.get('amount', 0)):
                self._record_order_state(
                    order_link_id,
                    OrderState.PARTIAL,
                    fill_qty=filled_qty,
                    fill_price=avg_price
                )
                is_filled = False
            else:
                is_filled = False

            result = {
                'filled': is_filled,
                'status': status,
                'fill_qty': filled_qty,
                'fill_price': avg_price,
                'order_id': order.get('id'),
                'client_order_id': order_link_id,
                'fees': fee_info,
                'latency_ms': round(latency_ms, 1)
            }

            self.logger.info(f"[VERIFY] linkId={order_link_id} status={status} filled={filled_qty} @ {avg_price} latency={latency_ms:.1f}ms")
            return result

        except Exception as e:
            self.logger.error(f"[VERIFY] Error verifying linkId={order_link_id}: {e}")
            return {'filled': False, 'status': 'error', 'error': str(e)}

    def connect(self) -> bool:
        """Connect to Bybit (REST + optional WebSocket)"""
        try:
            # REST connection via ccxt (always needed for market data)
            self.exchange = ccxt.bybit({
                'apiKey': self.config['api_key'],
                'secret': self.config['api_secret'],
                'sandbox': self.config.get('testnet', False),
                'enableRateLimit': True,
                'options': {
                    'defaultType': 'swap',  # USDT perpetual futures
                    'brokerId': 'Nu000450'  # Affiliate broker ID
                }
            })

            # Test connection
            self.exchange.load_markets()
            self.logger.info("Successfully connected to Bybit (REST)")

            # Cache tick sizes, qty steps, and min order values for WebSocket order manager
            self._market_tick_sizes = {}
            self._market_qty_steps = {}
            self._market_min_order_values = {}
            for sym, market in self.exchange.markets.items():
                bybit_sym = sym.replace("/", "").replace(":USDT", "")
                prec = market.get('precision', {})
                tick = prec.get('price')
                step = prec.get('amount')
                if isinstance(tick, float) and tick > 0:
                    self._market_tick_sizes[bybit_sym] = tick
                if isinstance(step, float) and step > 0:
                    self._market_qty_steps[bybit_sym] = step
                min_cost = market.get('limits', {}).get('cost', {}).get('min')
                if min_cost and isinstance(min_cost, (int, float)) and min_cost > 0:
                    self._market_min_order_values[bybit_sym] = min_cost

            # WebSocket connection for orders (optional, faster)
            if self.websocket_orders_enabled:
                self.ws_order_manager = WebSocketOrderManager(
                    api_key=self.config['api_key'],
                    api_secret=self.config['api_secret'],
                    testnet=self.config.get('testnet', False),
                    logger=self.logger
                )
                if self.ws_order_manager.connect():
                    # Populate WS manager with tick sizes from loaded markets
                    for bybit_sym, tick in self._market_tick_sizes.items():
                        qty_step = self._market_qty_steps.get(bybit_sym, 1.0)
                        self.ws_order_manager.set_precision(bybit_sym, tick, qty_step)
                    self.logger.info(f"WebSocket orders ENABLED - {len(self._market_tick_sizes)} tick sizes loaded")
                else:
                    self.logger.warning("WebSocket connection failed - falling back to REST")
                    self.websocket_orders_enabled = False

            # WebSocket data streams (real-time market data — 0ms reads)
            if self.websocket_orders_enabled or self.websocket_data_enabled:
                self._ws_data = WebSocketDataManager(
                    api_key=self.config['api_key'],
                    api_secret=self.config['api_secret'],
                    testnet=self.config.get('testnet', False),
                    logger=self.logger,
                    record_l2=self.config.get('record_l2', False),
                    l2_base=self.config.get('l2_base', 'data/l2'),
                    l2_min_interval=float(self.config.get('l2_min_interval', 0.5)),
                )
                if self._ws_data.connect():
                    self.logger.info("WebSocket DATA streams ENABLED (orderbook, trades, tickers, positions)")
                else:
                    self.logger.warning("WebSocket data streams failed — using REST fallback")
                    self._ws_data = None

            return True

        except Exception as e:
            self.logger.error(f"Failed to connect to Bybit: {e}")
            return False
            
    def setup_hedge_mode(self) -> bool:
        """Setup hedge position mode globally"""
        try:
            # Set hedge mode globally like original
            self.exchange.set_position_mode(hedged=True)
            self.logger.info("Set hedge position mode globally")
            return True
        except Exception as e:
            if "Position mode is not modified" in str(e):
                self.logger.info("Hedge mode already enabled")
                return True
            self.logger.error(f"Failed to set hedge mode: {e}")
            return False

    def set_leverage(self, symbol: str, leverage: int) -> bool:
        """Set leverage for a symbol on Bybit. Applies to both sides in hedge mode."""
        try:
            self._rate_limit()
            bybit_sym = symbol.replace("/", "").replace(":USDT", "")
            # Buy side (long)
            self.exchange.set_leverage(leverage, bybit_sym, {'buyLeverage': str(leverage), 'sellLeverage': str(leverage)})
            self.logger.info(f"[{bybit_sym}] Leverage set to {leverage}x")
            return True
        except Exception as e:
            if "Not modified" in str(e) or "not modified" in str(e):
                self.logger.info(f"[{bybit_sym}] Leverage already at {leverage}x")
                return True
            self.logger.warning(f"[{bybit_sym}] Could not set leverage: {e}")
            return False

    def get_max_leverage(self, symbol: str) -> Optional[float]:
        """Return the exchange's MAX allowed leverage for a symbol.

        Reads ccxt market info (``market(symbol)['limits']['leverage']['max']``).
        Returns None if it can't be determined so callers can fall back to the
        configured value. Used to clamp set_leverage so a per-symbol cap (e.g.
        ALLO max 25x) is honored instead of silently failing.
        """
        try:
            market = self.exchange.market(symbol)
            lev = (
                market.get('limits', {})
                      .get('leverage', {})
                      .get('max')
            )
            if lev is not None and float(lev) > 0:
                return float(lev)
        except Exception as e:
            self.logger.warning(f"[{symbol}] get_max_leverage failed: {e}")
        return None

    def fetch_order_status(self, order_id: str, symbol: str) -> Optional[Dict]:
        """Fetch a single order's status from the exchange by exchange order id.

        Returns a normalized dict::

            {"id", "status", "filled", "amount", "average", "is_filled"}

        ``is_filled`` is True only when ccxt reports status 'closed' with a
        positive filled qty (a real exchange fill). Returns None on error / not
        found so callers can treat "unknown" conservatively (NOT filled).
        """
        try:
            self._rate_limit()
            # Bybit's ccxt fetchOrder requires acknowledging its 500-order
            # lookback limitation, else it RAISES instead of returning the
            # order. Recently-placed grid orders are well within that window.
            order = self.exchange.fetch_order(
                order_id, symbol, params={"acknowledged": True}
            )
            if not order:
                return None
            status = order.get('status', 'unknown')
            filled = float(order.get('filled') or 0)
            amount = float(order.get('amount') or 0)
            avg = order.get('average')
            avg = float(avg) if avg else None
            is_filled = (status == 'closed' and filled > 0)
            return {
                "id": order.get('id'),
                "status": status,
                "filled": filled,
                "amount": amount,
                "average": avg,
                "is_filled": is_filled,
            }
        except Exception as e:
            self.logger.warning(
                f"[{symbol}] fetch_order_status({order_id}) failed: {e}"
            )
            return None

    def _rate_limit(self):
        """Apply rate limiting"""
        time.sleep(self.rate_limit_delay)

    def get_recent_executions(self, symbol: str, limit: int = 100) -> list:
        """Recent FILLS (Bybit v5 execution list) for a symbol, NEWEST-FIRST,
        normalized to ``[{"side": "buy"|"sell", "qty": float,
        "price": float, "exec_time": epoch_seconds, ...}, ...]``.

        Used by the strategy's startup adoption to derive the REAL open time of
        the CURRENT net position (walk fills newest→oldest to the last flat /
        flip boundary). This is exchange GROUND-TRUTH — unlike a position's
        ``createdTime`` (the symbol-slot's original creation, which does NOT
        reset as the net position churns open/close/flip).

        GUARDED: returns ``[]`` on any error so the caller falls back safely
        (never crashes the trading loop, never seeds a bogus old time)."""
        if self._ws_data:
            try:
                bybit_sym = symbol.replace("/", "").replace(":USDT", "")
                cached = self._ws_data.get_recent_executions(bybit_sym)
                if cached:
                    return cached[:int(limit)]
            except Exception:
                pass
        try:
            self._rate_limit()
            ccxt_sym = self.convert_symbol_format(symbol)
            # ccxt maps Bybit v5 GET /v5/execution/list under fetch_my_trades.
            trades = self.exchange.fetch_my_trades(
                ccxt_sym, limit=int(limit), params={"category": "linear"}
            )
            out = []
            for t in trades or []:
                info = t.get("info", {}) if isinstance(t, dict) else {}
                side = (t.get("side") or info.get("side") or "").lower()
                if side not in ("buy", "sell"):
                    continue
                qty = t.get("amount")
                if qty is None:
                    qty = info.get("execQty")
                try:
                    qty = float(qty or 0.0)
                except (TypeError, ValueError):
                    qty = 0.0
                price = t.get("price")
                if price is None:
                    price = info.get("execPrice")
                try:
                    price = float(price or 0.0)
                except (TypeError, ValueError):
                    price = 0.0
                # ms epoch from ccxt 'timestamp', fall back to v5 execTime (ms).
                ts_ms = t.get("timestamp")
                if ts_ms is None:
                    ts_ms = info.get("execTime")
                try:
                    exec_time = float(ts_ms) / 1000.0 if ts_ms else 0.0
                except (TypeError, ValueError):
                    exec_time = 0.0
                out.append({
                    "side": side,
                    "qty": qty,
                    "price": price,
                    "exec_time": exec_time,
                    "order_id": t.get("order") or info.get("orderId"),
                    "order_link_id": info.get("orderLinkId"),
                })
            # Newest-first (the adoption walk-back expects newest→oldest).
            out.sort(key=lambda e: e["exec_time"], reverse=True)
            return out
        except Exception as e:
            self.logger.warning(f"get_recent_executions failed for {symbol}: {e}")
            return []

    def convert_symbol_format(self, symbol: str) -> str:
        """Convert DOGEUSDT format to DOGE/USDT:USDT for futures trading"""
        if symbol.endswith('USDT') and '/' not in symbol:
            # Convert DOGEUSDT -> DOGE/USDT:USDT for futures
            base = symbol[:-4]  # Remove USDT suffix
            return f"{base}/USDT:USDT"
        return symbol

    # === WebSocket Data — 0ms cached reads ===

    def subscribe_data(self, symbol: str):
        """Subscribe to real-time WS data streams for a symbol."""
        if self._ws_data:
            bybit_sym = symbol.replace("/", "").replace(":USDT", "")
            self._ws_data.subscribe(bybit_sym)

    def get_current_price_cached(self, symbol: str) -> float:
        """Get price from WS cache (0ms) or REST fallback."""
        if self._ws_data:
            bybit_sym = symbol.replace("/", "").replace(":USDT", "")
            ticker = self._ws_data.get_ticker(bybit_sym)
            if ticker and ticker.get('last', 0) > 0:
                return ticker['last']
        return self.get_current_price(symbol)

    def get_orderbook_cached(self, symbol: str, limit: int = 50) -> Dict:
        """Get orderbook from WS cache (0ms) or REST fallback."""
        if self._ws_data:
            bybit_sym = symbol.replace("/", "").replace(":USDT", "")
            ob = self._ws_data.get_orderbook(bybit_sym)
            if ob and ob.get('bids') and ob.get('asks'):
                return ob
        return self.get_orderbook(symbol, limit)

    def get_positions_cached(self, symbol: str) -> Dict:
        """Get positions from WS cache (0ms) or REST fallback.

        WS position updates only arrive when positions CHANGE, so pre-initialized
        zeroed defaults may mask existing positions opened before WS connected.
        We seed from REST on first call per symbol to avoid this.
        """
        if self._ws_data:
            bybit_sym = symbol.replace("/", "").replace(":USDT", "")
            pos = self._ws_data.get_positions(bybit_sym)
            if pos:
                has_qty = (
                    float(pos.get('long', {}).get('qty', 0)) != 0
                    or float(pos.get('short', {}).get('qty', 0)) != 0
                )
                if has_qty:
                    return pos
                # WS has zeroed defaults — seed from REST once
                if bybit_sym not in self._ws_data._position_seeded:
                    rest_pos = self.get_positions(symbol)
                    if rest_pos:
                        self._ws_data._positions[bybit_sym] = rest_pos
                        self._ws_data._position_seeded.add(bybit_sym)
                        return rest_pos
                    self._ws_data._position_seeded.add(bybit_sym)
                    return pos  # REST also says zero — trust it
                return pos  # Already seeded, WS says zero — trust it
        return self.get_positions(symbol)

    def get_recent_trades_cached(self, symbol: str, limit: int = 200) -> List[Dict]:
        """Get recent trades from WS cache (0ms) or REST fallback."""
        if self._ws_data:
            bybit_sym = symbol.replace("/", "").replace(":USDT", "")
            trades = self._ws_data.get_recent_trades(bybit_sym)
            if trades:
                return trades[-limit:]
        return self.get_recent_trades(symbol, limit)

    # === CCXT-compatible cached methods (for strategy use) ===

    def fetch_positions_cached(self, symbols: list = None) -> list:
        """CCXT-compatible fetch_positions from WS cache (0ms) or REST fallback.

        Returns list of CCXT-style position dicts with 'symbol', 'side',
        'contracts', 'entryPrice', 'liquidationPrice' fields.
        """
        if self._ws_data and symbols:
            result = []
            for sym in symbols:
                bybit_sym = sym.replace("/", "").replace(":USDT", "")
                pos = self._ws_data.get_positions(bybit_sym)
                if pos:
                    for side_key in ('long', 'short'):
                        side_data = pos.get(side_key, {})
                        qty = side_data.get('qty', 0)
                        if qty > 0:
                            result.append({
                                'symbol': sym,
                                'side': side_key,
                                'contracts': qty,
                                'entryPrice': side_data.get('entry_price', 0),
                                'liquidationPrice': side_data.get('liq_price', 0),
                                'info': {'positionIdx': 1 if side_key == 'long' else 2},
                            })
                    if result:
                        return result
            # WS had no data — fallback
        return self.exchange.fetch_positions(symbols)

    def fetch_order_book_cached(self, symbol: str, limit: int = 50) -> Dict:
        """CCXT-compatible fetch_order_book from WS cache (0ms) or REST fallback."""
        if self._ws_data:
            bybit_sym = symbol.replace("/", "").replace(":USDT", "")
            ob = self._ws_data.get_orderbook(bybit_sym)
            if ob and ob.get('bids') and ob.get('asks'):
                return ob
        return self.exchange.fetch_order_book(symbol, limit)

    def fetch_trades_cached(self, symbol: str, since=None, limit: int = 50, params=None) -> list:
        """CCXT-compatible fetch_trades from WS cache (0ms) or REST fallback.

        Returns list of trade dicts. WS trades use {price, qty, side, timestamp}
        format — callers that need CCXT-style dicts should handle both.
        """
        if self._ws_data:
            bybit_sym = symbol.replace("/", "").replace(":USDT", "")
            trades = self._ws_data.get_recent_trades(bybit_sym)
            if trades:
                return trades[-limit:]
        return self.exchange.fetch_trades(symbol, since, limit, params or {})

    def get_balance(self) -> float:
        """Get USDT balance"""
        try:
            self._rate_limit()
            balance = self.exchange.fetch_balance()
            return balance.get('USDT', {}).get('total', 0.0)
        except Exception as e:
            self.logger.error(f"Error fetching balance: {e}")
            return 0.0

    def get_available_margin(self) -> float:
        """Get USDT available margin (free to deploy). Respects leverage —
        shrinks as positions are opened and as they go underwater."""
        try:
            self._rate_limit()
            balance = self.exchange.fetch_balance()
            return balance.get('USDT', {}).get('free', 0.0)
        except Exception as e:
            self.logger.error(f"Error fetching available margin: {e}")
            return 0.0
            
    def get_current_price(self, symbol: str) -> float:
        """Get current price for symbol"""
        try:
            self._rate_limit()
            ticker = self.exchange.fetch_ticker(symbol)
            return ticker['last']
        except Exception as e:
            self.logger.error(f"Error fetching price for {symbol}: {e}")
            return 0.0

    def get_ticker(self, symbol: str) -> Dict:
        """Get ticker data for symbol"""
        try:
            self._rate_limit()
            return self.exchange.fetch_ticker(symbol)
        except Exception as e:
            self.logger.error(f"Error fetching ticker for {symbol}: {e}")
            return {'last': 0.0}

    def get_orderbook(self, symbol: str, limit: int = 50) -> Dict:
        """Get orderbook depth for symbol"""
        try:
            self._rate_limit()
            return self.exchange.fetch_order_book(symbol, limit=limit)
        except Exception as e:
            self.logger.error(f"Error fetching orderbook for {symbol}: {e}")
            return {'bids': [], 'asks': []}

    def get_klines(self, symbol: str, interval: str = '1m', limit: int = 20) -> List[Dict]:
        """Get OHLCV klines via ccxt. Returns list of {timestamp, open, high, low, close}."""
        try:
            self._rate_limit()
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe=interval, limit=limit)
            return [
                {'timestamp': c[0], 'open': c[1], 'high': c[2], 'low': c[3], 'close': c[4]}
                for c in ohlcv
            ]
        except Exception as e:
            self.logger.warning(f"Error fetching klines for {symbol}: {e}")
            return []

    def get_recent_trades(self, symbol: str, limit: int = 200) -> List[Dict]:
        """Get recent public trades. Returns list of {price, qty, side, timestamp}."""
        try:
            self._rate_limit()
            trades = self.exchange.fetch_trades(symbol, limit=limit)
            return [
                {
                    'price': float(t['price']),
                    'qty': float(t['amount']),
                    'side': t['side'],
                    'timestamp': float(t['timestamp']) / 1000.0 if t['timestamp'] > 1e12 else float(t['timestamp']),
                }
                for t in trades
            ]
        except Exception as e:
            self.logger.warning(f"Error fetching recent trades for {symbol}: {e}")
            return []

    def get_fee_rate(self, symbol: str) -> Optional[Dict]:
        """Get account fee rate for a symbol. Returns {maker_fee_bps, taker_fee_bps} or None."""
        try:
            self._rate_limit()
            bybit_sym = symbol.replace("/", "").replace(":USDT", "")
            resp = self.exchange.privateGetV5AccountFeeRate({'category': 'linear', 'symbol': bybit_sym})
            if resp and str(resp.get('retCode')) == '0':
                items = resp.get('result', {}).get('list', [])
                if items:
                    maker = float(items[0].get('makerFeeRate', 0))
                    taker = float(items[0].get('takerFeeRate', 0))
                    return {
                        'maker_fee_bps': round(maker * 10000, 2),
                        'taker_fee_bps': round(taker * 10000, 2),
                    }
            return None
        except Exception as e:
            self.logger.warning(f"Error fetching fee rate for {symbol}: {e}")
            return None

    def get_positions(self, symbol, return_none_on_error: bool = False) -> dict:
        """Get positions for symbol - matches original format.

        ``return_none_on_error=True`` (opt-in; default False keeps EVERY
        existing caller byte-identical) makes a REST failure return None
        instead of the all-zero template — so a caller can distinguish a
        genuine flat position from an unreadable one. The streak-heal logic
        uses this: writing zeros from a swallowed error into the WS cache
        would falsely declare a live position flat and stop managing it."""
        values = {
            "long": {
                "qty": 0.0,
                "price": 0.0,
                "realised": 0,
                "cum_realised": 0,
                "upnl": 0,
                "upnl_pct": 0,
                "liq_price": 0,
                "entry_price": 0,
            },
            "short": {
                "qty": 0.0,
                "price": 0.0,
                "realised": 0,
                "cum_realised": 0,
                "upnl": 0,
                "upnl_pct": 0,
                "liq_price": 0,
                "entry_price": 0,
            },
        }
        try:
            self._rate_limit()
            data = self.exchange.fetch_positions([symbol])
            if len(data) >= 1:
                # Process each position using positionIdx (hedge mode: 1=long, 2=short)
                # IMPORTANT: Use positionIdx NOT side, because side="None" when flat!
                for pos in data:
                    position_idx = pos['info'].get('positionIdx')

                    if position_idx == '1' or position_idx == 1:
                        side_key = 'long'
                    elif position_idx == '2' or position_idx == 2:
                        side_key = 'short'
                    else:
                        self.logger.warning(f"Unknown positionIdx: {position_idx}, skipping")
                        continue

                    # Parse position data (works for both open and flat positions)
                    values[side_key]["qty"] = float(pos["contracts"])
                    values[side_key]["price"] = float(pos["entryPrice"] or 0)
                    values[side_key]["realised"] = round(float(pos["info"]["unrealisedPnl"] or 0), 4)
                    values[side_key]["cum_realised"] = round(float(pos["info"]["cumRealisedPnl"] or 0), 4)
                    values[side_key]["upnl"] = round(float(pos["info"]["unrealisedPnl"] or 0), 4)
                    values[side_key]["upnl_pct"] = round(float(pos["percentage"] or 0), 4)
                    values[side_key]["liq_price"] = float(pos["liquidationPrice"] or 0)
                    values[side_key]["entry_price"] = float(pos["entryPrice"] or 0)

            self.logger.info(f"Positions for {symbol}: Long={values['long']['qty']}, Short={values['short']['qty']}")
        except Exception as e:
            self.logger.error(f"Error getting positions for {symbol}: {e}")
            if return_none_on_error:
                return None
        return values
            
    def get_open_orders(self, symbol: str = None) -> List[Dict]:
        """Get open orders"""
        try:
            self._rate_limit()
            if symbol:
                orders = self.exchange.fetch_open_orders(symbol)
            else:
                orders = self.exchange.fetch_open_orders()
                
            formatted_orders = []
            for order in orders:
                formatted_orders.append({
                    'id': order['id'],
                    'symbol': order['symbol'],
                    'side': order['side'],
                    'amount': order['amount'],
                    'price': order['price'],
                    'type': order['type'],
                    'reduce_only': order.get('reduceOnly', False),
                    'status': order['status']
                })
            return formatted_orders
        except Exception as e:
            self.logger.error(f"Error fetching open orders: {e}")
            return []
            
    def place_order(
        self,
        symbol: str,
        side: str,
        amount: float,
        price: float,
        order_type: str = "limit",
        reduce_only: bool = False,
        post_only: bool = False,
        position_idx: int = None
    ) -> Dict:
        """Place an order

        Args:
            position_idx: Optional explicit position index for hedge mode
                         1 = long position, 2 = short position
                         If not provided, will be auto-determined from side (buy=1, sell=2)
        """
        try:
            # === INSTITUTIONAL ORDER TRACKING ===
            # Generate unique orderLinkId for exchange-enforced idempotency
            # (No bandaid dedup needed - exchange rejects duplicate orderLinkIds)
            t_start = time.perf_counter()
            order_link_id = self._generate_order_link_id(symbol, side)

            # Record PENDING state
            self._record_order_state(
                order_link_id,
                OrderState.PENDING,
                symbol=symbol,
                side=side,
                amount=amount,
                price=price
            )

            # Snap price and qty to exchange precision
            precision_amount, precision_price, min_amount = self.get_precision_and_limits(symbol)
            if precision_amount is None:
                self.logger.error(f"Could not get precision for {symbol}")
                return {}

            amount = self._snap_qty(symbol, amount)
            if price is not None:
                price = self._snap_to_tick(symbol, price)

            # Check minimum quantity
            if min_amount and amount < min_amount:
                self.logger.error(f"Order quantity {amount} below minimum {min_amount}")
                return {}

            # Safety net: check notional value meets exchange minimum
            # Skip for reduce_only orders — must be able to close sub-minimum positions
            if price is not None and not reduce_only:
                notional = amount * price
                bybit_sym = symbol.replace("/", "").replace(":USDT", "")
                min_val = getattr(self, '_market_min_order_values', {}).get(bybit_sym, 5.0)
                if notional < min_val:
                    self.logger.warning(
                        f"SAFETY NET: {symbol} notional ${notional:.2f} < min ${min_val} "
                        f"(qty={amount}, price={price}) — SKIPPED")
                    return {}

            # Setup hedge mode once if not already done
            if not hasattr(self, '_hedge_mode_set'):
                self.setup_hedge_mode()
                self._hedge_mode_set = True

            # Determine position_idx: use explicit value if provided, otherwise auto-determine
            # For hedge mode: buy=1 (long), sell=2 (short) when OPENING positions
            # For close orders, position_idx should be explicitly provided
            if position_idx is None:
                position_idx = 1 if side == 'buy' else 2

            # === TRY WEBSOCKET FIRST (faster) ===
            if self.websocket_orders_enabled and self.ws_order_manager:
                # Record SENT state before API call
                self._record_order_state(order_link_id, OrderState.SENT)

                ws_result = self.ws_order_manager.place_order(
                    symbol=symbol,
                    side=side,
                    amount=amount,
                    price=price,
                    order_type=order_type,
                    reduce_only=reduce_only,
                    post_only=post_only,
                    position_idx=position_idx,
                    order_link_id=order_link_id  # INSTITUTIONAL: exchange-enforced idempotency
                )

                t_end = time.perf_counter()
                latency_ms = (t_end - t_start) * 1000
                self._record_latency(latency_ms)

                # SAFETY NET (Bybit 110017): the WS layer surfaces a reduce-only
                # "qty truncated to zero" as a distinct already-flat outcome.
                # Pass it straight through (NOT a confirmed order, NOT a REST
                # retry) so the strategy clears the level and stops re-posting.
                if ws_result and ws_result.get("retCode") == 110017:
                    self._record_order_state(
                        order_link_id, OrderState.REJECTED, latency_ms=latency_ms)
                    self.logger.info(
                        f"[WS] reduce-only on flat (110017) linkId={order_link_id} "
                        f"— treating as already-closed, no retry")
                    return {"error": "reduce_only_on_flat", "retCode": 110017}

                if ws_result and ws_result.get('id'):
                    # Record CONFIRMED state
                    self._record_order_state(
                        order_link_id,
                        OrderState.CONFIRMED,
                        order_id=ws_result.get('id'),
                        latency_ms=latency_ms
                    )
                    ws_result['clientOrderId'] = order_link_id
                    ws_result['latency_ms'] = round(latency_ms, 1)

                    # === REGISTER ORDER IN CENTRAL REGISTRY ===
                    self._order_registry[order_link_id] = {
                        'side': side,
                        'price': price,
                        'qty': amount,
                        'order_id': ws_result.get('id'),
                        'placed_at': time.time()
                    }

                    return ws_result
                else:
                    # WS returned empty — either disconnected or exchange rejected
                    self._record_order_state(order_link_id, OrderState.REJECTED, latency_ms=latency_ms)
                    if not self.ws_order_manager.connected:
                        # WS disconnected — fall through to REST
                        self.logger.warning(
                            f"[WS] Disconnected, falling back to REST for {side} {amount} @ {price}")
                    else:
                        # WS connected but exchange rejected — don't retry via REST
                        self.logger.warning(f"[WS] Order failed linkId={order_link_id} latency={latency_ms:.1f}ms")
                        return {}

            # === REST ONLY (when WebSocket disabled) ===
            self._rate_limit()

            # Record SENT state before REST call
            self._record_order_state(order_link_id, OrderState.SENT)

            params = {
                'positionIdx': position_idx,
                'orderLinkId': order_link_id  # INSTITUTIONAL: exchange-enforced idempotency
            }
            if reduce_only:
                params['reduceOnly'] = True
            if post_only:
                params['postOnly'] = True

            # Debug logging for close orders
            if reduce_only:
                self.logger.info(f"Creating reduceOnly order: {symbol} {side} {amount} @ {price}, params={params}")

            order = self.exchange.create_order(
                symbol=symbol,
                type=order_type,
                side=side,
                amount=amount,
                price=price,
                params=params
            )

            t_end = time.perf_counter()
            latency_ms = (t_end - t_start) * 1000
            self._record_latency(latency_ms)

            # Record CONFIRMED state
            self._record_order_state(
                order_link_id,
                OrderState.CONFIRMED,
                order_id=order['id'],
                latency_ms=latency_ms
            )

            # === REGISTER ORDER IN CENTRAL REGISTRY ===
            self._order_registry[order_link_id] = {
                'side': side,
                'price': price,
                'qty': amount,
                'order_id': order['id'],
                'placed_at': time.time()
            }

            # Log order placement (handle market orders where price is None)
            if price is not None:
                self.logger.info(f"[REST] Placed {side} order: {symbol} {amount} @ {price} linkId={order_link_id} latency={latency_ms:.1f}ms")
            else:
                self.logger.info(f"[REST] Placed {side} MARKET order: {symbol} {amount} linkId={order_link_id} latency={latency_ms:.1f}ms")
            return {
                'id': order['id'],
                'clientOrderId': order_link_id,
                'symbol': order['symbol'],
                'side': order['side'],
                'amount': order['amount'],
                'price': order['price'],
                'status': order['status'],
                'latency_ms': round(latency_ms, 1)
            }

        except Exception as e:
            t_end = time.perf_counter()
            latency_ms = (t_end - t_start) * 1000
            self._record_order_state(order_link_id, OrderState.REJECTED, error=str(e), latency_ms=latency_ms)
            self.logger.error(f"Error placing order linkId={order_link_id}: {e} latency={latency_ms:.1f}ms")
            # SAFETY NET (Bybit 110017): the REST create_order raises with
            # "110017" / "truncated to zero" when a reduce-only order's qty would
            # truncate to zero — i.e. the position is already flat. Surface it as
            # the SAME distinct already-flat outcome the WS path returns, so the
            # strategy clears the pending order/level and does NOT retry it.
            if "110017" in str(e):
                return {"error": "reduce_only_on_flat", "retCode": 110017}
            # surface the error to the caller (was bare {}) so strategies can
            # detect permanent rejects like 110126 "must sign agreement" and
            # skip the contract instead of blindly retrying.
            return {"error": str(e)[:200]}

    def cancel_order(self, order_id: str = None, symbol: str = None, order_link_id: str = None) -> bool:
        """Cancel specific order by orderId OR orderLinkId (preferred).

        orderLinkId is the professional approach - we generate it locally,
        so no API latency needed to get the order ID.
        """
        try:
            id_str = order_link_id or order_id

            # === TRY WEBSOCKET FIRST (faster) ===
            if self.websocket_orders_enabled and self.ws_order_manager:
                if self.ws_order_manager.cancel_order(order_id=order_id, symbol=symbol, order_link_id=order_link_id):
                    # Unregister from order registry on successful cancel
                    if order_link_id:
                        self.unregister_order(order_link_id)
                    return True
                else:
                    self.logger.debug(f"WS cancel failed for {id_str}, falling back to REST")

            # === REST FALLBACK ===
            self._rate_limit()
            params = {}
            if order_link_id:
                params['orderLinkId'] = order_link_id
            self.exchange.cancel_order(order_id or '', symbol, params=params)
            self.logger.info(f"[REST] Cancelled {id_str} for {symbol}")
            # Unregister from order registry on successful cancel
            if order_link_id:
                self.unregister_order(order_link_id)
            return True
        except Exception as e:
            id_str = order_link_id or order_id
            err_str = str(e).lower()
            # Order already gone (filled/cancelled) - unregister and don't log error
            if 'order not found' in err_str or 'does not exist' in err_str or 'not exists' in err_str or '110001' in str(e):
                if order_link_id:
                    self.unregister_order(order_link_id)
                self.logger.debug(f"Order {id_str} already gone (filled/cancelled)")
                return True  # Treat as success - order is gone
            self.logger.error(f"Error cancelling {id_str}: {e}")
            return False

    def amend_order(
        self,
        symbol: str,
        order_id: str = None,
        order_link_id: str = None,
        qty: float = None,
        price: float = None,
    ) -> bool:
        """
        Amend order price/qty in-place. Zero naked window — order stays on book.

        Tries WS first, falls back to REST (ccxt edit_order).
        Returns True on success, False if order is gone or amend failed.
        """
        try:
            id_str = order_link_id or order_id

            # === TRY WEBSOCKET FIRST ===
            if self.websocket_orders_enabled and self.ws_order_manager:
                result = self.ws_order_manager.amend_order(
                    symbol=symbol,
                    order_id=order_id,
                    order_link_id=order_link_id,
                    qty=qty,
                    price=price,
                )
                if result:
                    return True
                else:
                    self.logger.debug(f"WS amend failed for {id_str}, falling back to REST")

            # === REST FALLBACK ===
            if not order_id:
                self.logger.warning(f"REST amend needs orderId, have orderLinkId={order_link_id}")
                return False

            self._rate_limit()
            params = {}
            if order_link_id:
                params['orderLinkId'] = order_link_id

            amend_params = {'category': 'linear'}
            if qty is not None:
                qty = self._snap_qty(symbol, qty)
                amend_params['qty'] = str(int(qty))
            if price is not None:
                price = self._snap_to_tick(symbol, price)
                amend_params['price'] = str(price)
            amend_params.update(params)

            self.exchange.edit_order(
                order_id,
                symbol,
                type='limit',
                side=None,
                amount=qty,
                price=price,
                params=amend_params,
            )
            self.logger.info(f"[REST-AMEND] {id_str} → price={price} qty={qty}")
            return True

        except Exception as e:
            id_str = order_link_id or order_id
            err_str = str(e).lower()
            if 'order not found' in err_str or 'not exists' in err_str or '110001' in str(e):
                self.logger.debug(f"Amend: order {id_str} already gone")
                return False
            self.logger.warning(f"Amend failed for {id_str}: {e}")
            return False

    def place_orders_batch(self, orders: List[Dict], symbol: str, position_idx: int = 0, skip_cancel_ids: Optional[List[str]] = None) -> List[Dict]:
        """
        Place multiple orders via WebSocket batch API.

        8x faster than sequential: 190ms vs 1.5s for 8 orders.

        Args:
            orders: List of dicts with: side, amount, price, order_link_id (optional)
            symbol: Trading symbol
            position_idx: Position index for hedge mode

        Returns:
            List of result dicts with success/failure per order
        """
        if not self.websocket_orders_enabled or not self.ws_order_manager:
            self.logger.warning("[BATCH] WebSocket not available, falling back to sequential")
            # Fallback to sequential
            results = []
            for order in orders:
                result = self.place_order(
                    symbol=symbol,
                    side=order.get('side'),
                    amount=order.get('amount'),
                    price=order.get('price'),
                    order_type='limit',
                    post_only=order.get('post_only', True),
                    reduce_only=order.get('reduce_only', False),
                    order_link_id=order.get('order_link_id')
                )
                results.append({'success': bool(result), 'order': result})
            return results

        return self.ws_order_manager.place_orders_batch(orders, symbol, position_idx)

    def cancel_orders_batch(self, orders: List[Dict], symbol: str) -> List[bool]:
        """
        Cancel multiple orders via WebSocket batch API.

        8x faster than sequential: 190ms vs 1.5s for 8 orders.

        Args:
            orders: List of dicts with: order_id or order_link_id
            symbol: Trading symbol

        Returns:
            List of bools indicating success/failure per cancel
        """
        if not self.websocket_orders_enabled or not self.ws_order_manager:
            self.logger.warning("[BATCH] WebSocket not available, falling back to sequential")
            # Fallback to sequential
            results = []
            for order in orders:
                success = self.cancel_order(
                    order_id=order.get('order_id'),
                    symbol=symbol,
                    order_link_id=order.get('order_link_id')
                )
                results.append(success)
            return results

        results = self.ws_order_manager.cancel_orders_batch(orders, symbol)

        # Unregister successfully cancelled orders
        for i, success in enumerate(results):
            if success and i < len(orders):
                order_link_id = orders[i].get('order_link_id')
                if order_link_id:
                    self.unregister_order(order_link_id)

        return results

    def cancel_all_orders(self, symbol: str = None) -> bool:
        """Cancel all orders for symbol or all symbols if symbol=None"""
        try:
            if symbol:
                # Cancel orders for specific symbol
                orders = self.get_open_orders(symbol)
                success_count = 0
                
                for order in orders:
                    if not order.get('reduce_only', False):  # Don't cancel TP orders
                        if self.cancel_order(order['id'], symbol):
                            success_count += 1
                            
                self.logger.info(f"Cancelled {success_count}/{len(orders)} orders for {symbol}")
                return success_count == len(orders)
            else:
                # Cancel ALL orders across ALL symbols - fetch all then cancel individually
                self.logger.info("Cancelling ALL orders across entire account...")
                self._rate_limit()
                
                # Get ALL open orders across all symbols
                all_orders = self.get_open_orders()  # No symbol = all orders
                self.logger.info(f"Found {len(all_orders)} total orders across all symbols")
                
                # Log details of what we found
                if all_orders:
                    symbols_found = {}
                    for order in all_orders:
                        symbol = order.get('symbol', 'UNKNOWN')
                        if symbol not in symbols_found:
                            symbols_found[symbol] = 0
                        symbols_found[symbol] += 1
                    self.logger.info(f"Orders breakdown: {symbols_found}")
                
                cancelled_count = 0
                symbols_with_orders = set()
                
                for order in all_orders:
                    try:
                        order_symbol = order.get('symbol', 'UNKNOWN')
                        symbols_with_orders.add(order_symbol)
                        
                        # Cancel the order
                        if self.cancel_order(order['id'], order_symbol):
                            cancelled_count += 1
                            self.logger.debug(f"[{order_symbol}] Cancelled order {order['id']}")
                        else:
                            self.logger.warning(f"[{order_symbol}] Failed to cancel order {order['id']}")
                            
                    except Exception as e:
                        self.logger.warning(f"Failed to cancel order {order.get('id', 'UNKNOWN')}: {e}")
                
                self.logger.info(f"✅ Cancelled {cancelled_count}/{len(all_orders)} orders across symbols: {sorted(symbols_with_orders)}")
                
                # Double-check: fetch orders again to see if any remain
                time.sleep(1)  # Give exchange time to process
                self._rate_limit()
                remaining_orders = self.get_open_orders()
                if remaining_orders:
                    remaining_symbols = {}
                    for order in remaining_orders:
                        symbol = order.get('symbol', 'UNKNOWN')
                        if symbol not in remaining_symbols:
                            remaining_symbols[symbol] = 0
                        remaining_symbols[symbol] += 1
                    self.logger.warning(f"⚠️  {len(remaining_orders)} orders still remain after cleanup: {remaining_symbols}")
                    
                    # Try to cancel remaining orders
                    for order in remaining_orders:
                        try:
                            order_symbol = order.get('symbol', 'UNKNOWN')
                            self.logger.info(f"Attempting to cancel remaining order: {order['id']} for {order_symbol}")
                            self.cancel_order(order['id'], order_symbol)
                        except Exception as e:
                            self.logger.error(f"Failed to cancel remaining order {order.get('id', 'UNKNOWN')}: {e}")
                else:
                    self.logger.info("✅ Confirmed: No orders remaining after cleanup")
                
                return cancelled_count == len(all_orders)
                
        except Exception as e:
            self.logger.error(f"Error cancelling all orders: {e}")
            return False

    # === ORDER REGISTRY METHODS ===
    # Universal order tracking - eliminates need for per-strategy registration

    def get_registered_orders(self) -> Dict[str, Dict]:
        """
        Get all registered orders (copy of registry).

        Returns:
            Dict[orderLinkId, {side, price, qty, order_id, placed_at}]
        """
        return self._order_registry.copy()

    def unregister_order(self, order_link_id: str) -> bool:
        """
        Remove order from registry (on fill/cancel).

        Args:
            order_link_id: The orderLinkId to unregister

        Returns:
            True if order was registered (and now removed), False if not found
        """
        if order_link_id in self._order_registry:
            del self._order_registry[order_link_id]
            return True
        return False

    def has_order_at_price(self, side: str, price: float, tolerance_bps: float = 1.0) -> bool:
        """
        Check if registry already has an order at/near this price.

        Used by strategies to avoid duplicate order placement (68+ placement sites).

        Args:
            side: 'buy' or 'sell'
            price: Price to check
            tolerance_bps: Tolerance in basis points (default 1 bps = 0.01%)

        Returns:
            True if similar order exists, False otherwise
        """
        tolerance_fraction = tolerance_bps / 10000.0  # Convert bps to fraction

        for order_link_id, order_data in self._order_registry.items():
            if order_data['side'] == side:
                order_price = order_data.get('price')
                if order_price is not None:
                    # Check if prices are within tolerance
                    price_diff = abs(order_price - price) / price
                    if price_diff <= tolerance_fraction:
                        return True
        return False

    def _place_order_with_position_side(
        self,
        symbol: str,
        side: str,
        amount: float,
        price: float,
        order_type: str = "limit",
        reduce_only: bool = False,
        position_side: str = None,
        post_only: bool = False
    ) -> Dict:
        """Place an order with explicit position side control"""
        try:
            # === INSTITUTIONAL ORDER TRACKING ===
            t_start = time.perf_counter()
            order_link_id = self._generate_order_link_id(symbol, side)

            # Record PENDING state
            self._record_order_state(
                order_link_id,
                OrderState.PENDING,
                symbol=symbol,
                side=side,
                amount=amount,
                price=price,
                position_side=position_side
            )

            # Snap price and qty to exchange precision
            precision_amount, precision_price, min_amount = self.get_precision_and_limits(symbol)
            if precision_amount is None:
                self.logger.error(f"Could not get precision for {symbol}")
                return {}

            amount = self._snap_qty(symbol, amount)
            if price is not None:
                price = self._snap_to_tick(symbol, price)

            # Check minimum quantity
            if min_amount and amount < min_amount:
                self.logger.error(f"Order quantity {amount} below minimum {min_amount}")
                return {}

            # Setup hedge mode once if not already done
            if not hasattr(self, '_hedge_mode_set'):
                self.setup_hedge_mode()
                self._hedge_mode_set = True

            # For reduce-only orders, use position side; for regular orders, use order side
            if reduce_only and position_side:
                position_idx = 1 if position_side == 'long' else 2
            else:
                position_idx = 1 if side == 'buy' else 2

            # === TRY WEBSOCKET FIRST (faster) ===
            if self.websocket_orders_enabled and self.ws_order_manager:
                self._record_order_state(order_link_id, OrderState.SENT)

                ws_result = self.ws_order_manager.place_order(
                    symbol=symbol,
                    side=side,
                    amount=amount,
                    price=price,
                    order_type=order_type,
                    reduce_only=reduce_only,
                    post_only=post_only,
                    position_idx=position_idx,
                    order_link_id=order_link_id
                )

                t_end = time.perf_counter()
                latency_ms = (t_end - t_start) * 1000
                self._record_latency(latency_ms)

                if ws_result:
                    self._record_order_state(
                        order_link_id,
                        OrderState.CONFIRMED,
                        order_id=ws_result.get('id'),
                        latency_ms=latency_ms
                    )
                    ws_result['clientOrderId'] = order_link_id
                    ws_result['latency_ms'] = round(latency_ms, 1)
                    return ws_result
                else:
                    self._record_order_state(order_link_id, OrderState.REJECTED, latency_ms=latency_ms)
                    self.logger.warning(f"[WS] Order failed linkId={order_link_id} latency={latency_ms:.1f}ms")
                    return {}

            # === REST ONLY (when WebSocket disabled) ===
            self._rate_limit()
            self._record_order_state(order_link_id, OrderState.SENT)

            params = {
                'positionIdx': position_idx,
                'orderLinkId': order_link_id
            }
            if reduce_only:
                params['reduceOnly'] = True
            if post_only:
                params['postOnly'] = True

            order = self.exchange.create_order(
                symbol=symbol,
                type=order_type,
                side=side,
                amount=amount,
                price=price,
                params=params
            )

            t_end = time.perf_counter()
            latency_ms = (t_end - t_start) * 1000
            self._record_latency(latency_ms)

            self._record_order_state(
                order_link_id,
                OrderState.CONFIRMED,
                order_id=order['id'],
                latency_ms=latency_ms
            )

            if price is not None:
                self.logger.info(f"[REST] Placed {side} order: {symbol} {amount} @ {price} linkId={order_link_id} latency={latency_ms:.1f}ms")
            else:
                self.logger.info(f"[REST] Placed {side} MARKET order: {symbol} {amount} linkId={order_link_id} latency={latency_ms:.1f}ms")
            return {
                'id': order['id'],
                'clientOrderId': order_link_id,
                'symbol': order['symbol'],
                'side': order['side'],
                'amount': order['amount'],
                'price': order['price'],
                'status': order['status'],
                'latency_ms': round(latency_ms, 1)
            }

        except Exception as e:
            t_end = time.perf_counter()
            latency_ms = (t_end - t_start) * 1000
            self._record_order_state(order_link_id, OrderState.REJECTED, error=str(e), latency_ms=latency_ms)
            self.logger.error(f"Error placing order linkId={order_link_id}: {e} latency={latency_ms:.1f}ms")
            return {}

    def place_take_profit_order(self, symbol: str, side: str, amount: float, price: float, position_side: str = None) -> Dict:
        """Place a take profit (reduce-only) order with post-only for maker fees"""
        # For reduce-only orders, we need to pass the position side explicitly
        # because positionIdx should match the position, not the order side
        return self._place_order_with_position_side(
            symbol=symbol,
            side=side,
            amount=amount,
            price=price,
            order_type="limit",
            reduce_only=True,
            position_side=position_side,
            post_only=True  # Use maker orders for better fees
        )

    def create_order(
        self,
        symbol: str,
        type: str,
        side: str,
        amount: float,
        price: float = None,
        params: Dict = None
    ) -> Dict:
        """
        Override ccxt's create_order to use WebSocket when available.
        This intercepts strategy calls that use ccxt-style create_order().
        No REST fallback - if WS fails, return empty to prevent duplicates.
        """
        params = params or {}

        # Extract position_idx from params (hedge mode)
        position_idx = params.get('positionIdx')
        if position_idx is None:
            position_idx = 1 if side == 'buy' else 2

        reduce_only = params.get('reduceOnly', False)
        post_only = params.get('postOnly', False)

        # === INSTITUTIONAL ORDER TRACKING ===
        t_start = time.perf_counter()
        order_link_id = self._generate_order_link_id(symbol, side)

        self._record_order_state(
            order_link_id,
            OrderState.PENDING,
            symbol=symbol,
            side=side,
            amount=amount,
            price=price
        )

        # DEBUG: Log every order attempt
        self.logger.info(f"[ORDER] {side} {amount} @ {price} | post_only={post_only} reduce_only={reduce_only} linkId={order_link_id}")

        # === TRY WEBSOCKET FIRST (faster) ===
        if self.websocket_orders_enabled and self.ws_order_manager and self.ws_order_manager.connected:
            try:
                self._record_order_state(order_link_id, OrderState.SENT)

                ws_result = self.ws_order_manager.place_order(
                    symbol=symbol,
                    side=side,
                    amount=amount,
                    price=price,
                    order_type=type,
                    reduce_only=reduce_only,
                    post_only=post_only,
                    position_idx=position_idx,
                    order_link_id=order_link_id
                )

                t_end = time.perf_counter()
                latency_ms = (t_end - t_start) * 1000
                self._record_latency(latency_ms)

                if ws_result and ws_result.get('id'):
                    self._record_order_state(
                        order_link_id,
                        OrderState.CONFIRMED,
                        order_id=ws_result.get('id'),
                        latency_ms=latency_ms
                    )
                    # Register in centralized registry for duplicate prevention
                    self._order_registry[order_link_id] = {
                        'side': side,
                        'price': price,
                        'qty': amount,
                        'order_id': ws_result.get('id'),
                        'placed_at': time.time()
                    }
                    self.logger.info(f"[REGISTRY] Added {order_link_id} orderId={ws_result.get('id')} -> registry now has {len(self._order_registry)} orders")
                    ws_result['clientOrderId'] = order_link_id
                    ws_result['latency_ms'] = round(latency_ms, 1)
                    return ws_result
                else:
                    self._record_order_state(order_link_id, OrderState.REJECTED, latency_ms=latency_ms)
                    self.logger.warning(f"[WS] Order failed linkId={order_link_id} latency={latency_ms:.1f}ms")
                    return {}
            except Exception as e:
                t_end = time.perf_counter()
                latency_ms = (t_end - t_start) * 1000
                self._record_order_state(order_link_id, OrderState.REJECTED, error=str(e), latency_ms=latency_ms)
                self.logger.warning(f"[WS] Exception linkId={order_link_id}: {e} latency={latency_ms:.1f}ms")
                return {}

        # === REST ONLY (when WebSocket disabled) ===
        self._record_order_state(order_link_id, OrderState.SENT)
        self.logger.info(f"[REST] Placing {side} {amount} @ {price} linkId={order_link_id}")
        self._rate_limit()

        params['orderLinkId'] = order_link_id
        result = self.exchange.create_order(
            symbol=symbol,
            type=type,
            side=side,
            amount=amount,
            price=price,
            params=params
        )

        t_end = time.perf_counter()
        latency_ms = (t_end - t_start) * 1000
        self._record_latency(latency_ms)

        if result and result.get('id'):
            self._record_order_state(
                order_link_id,
                OrderState.CONFIRMED,
                order_id=result.get('id'),
                latency_ms=latency_ms
            )
            # Register in centralized registry for duplicate prevention
            self._order_registry[order_link_id] = {
                'side': side,
                'price': price,
                'qty': amount,
                'order_id': result.get('id'),
                'placed_at': time.time()
            }
            result['clientOrderId'] = order_link_id
            result['latency_ms'] = round(latency_ms, 1)

        return result

    def create_limit_buy_order(self, symbol: str, amount: float, price: float, params: Dict = None) -> Dict:
        """Override ccxt convenience method to use WebSocket."""
        return self.create_order(symbol, 'limit', 'buy', amount, price, params)

    def create_limit_sell_order(self, symbol: str, amount: float, price: float, params: Dict = None) -> Dict:
        """Override ccxt convenience method to use WebSocket."""
        return self.create_order(symbol, 'limit', 'sell', amount, price, params)

    def create_market_buy_order(self, symbol: str, amount: float, params: Dict = None) -> Dict:
        """Override ccxt convenience method to use WebSocket."""
        return self.create_order(symbol, 'market', 'buy', amount, None, params)

    def create_market_sell_order(self, symbol: str, amount: float, params: Dict = None) -> Dict:
        """Override ccxt convenience method to use WebSocket."""
        return self.create_order(symbol, 'market', 'sell', amount, None, params)

    def __getattr__(self, name):
        """Delegate unknown methods to underlying ccxt exchange for backwards compatibility."""
        if hasattr(self, 'exchange') and self.exchange is not None:
            return getattr(self.exchange, name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        
