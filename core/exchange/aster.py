"""
Aster Exchange Implementation
Implementation for AsterDEX perpetual futures trading.
Binance Futures-compatible REST API at https://fapi.asterdex.com
WebSocket streams at wss://fstream.asterdex.com (Binance-compatible)

Supports the Vortex strategy.
"""

import requests
import time
import json
import hmac
import hashlib
import uuid
import threading
import math
from datetime import datetime
from typing import List, Dict, Optional
from .base import BaseExchange

try:
    import websocket
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False


class AsterWebSocketDataManager:
    """Real-time market data via AsterDEX WebSocket (Binance Futures-compatible).

    Public: wss://fstream.asterdex.com/ws
      - <symbol>@depth@100ms   → orderbook snapshot + delta
      - <symbol>@aggTrade      → trade feed
      - <symbol>@bookTicker    → best bid/ask (fastest ticker)

    Private: wss://fstream.asterdex.com/ws/<listenKey>
      - ORDER_TRADE_UPDATE     → order fills
      - ACCOUNT_UPDATE         → position & balance changes

    All data cached in memory. Strategy reads from cache (0ms) instead of REST (~250ms).
    Falls back to REST if WS disconnects.
    """

    PUBLIC_URL = "wss://fstream.asterdex.com/ws"
    PRIVATE_URL_BASE = "wss://fstream.asterdex.com/ws/"  # + listenKey

    def __init__(self, api_key: str, api_secret: str, base_url: str = "https://fapi.asterdex.com",
                 testnet: bool = False, logger=None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url
        self.testnet = testnet
        self.logger = logger

        # Cached data (thread-safe via GIL for simple dict assignment)
        self._orderbooks: Dict[str, Dict] = {}
        self._tickers: Dict[str, Dict] = {}
        self._positions: Dict[str, Dict] = {}
        self._recent_trades: Dict[str, list] = {}

        self._public_ws = None
        self._private_ws = None
        self._running = False
        self._connected_public = False
        self._connected_private = False
        self._subscribed_symbols: List[str] = []  # lowercase: "asterusdt"
        self._listen_key: Optional[str] = None
        self._listen_key_ts: float = 0
        self._sub_id = 1

        self._default_position_side = {
            "qty": 0.0, "price": 0.0, "realised": 0, "cum_realised": 0,
            "upnl": 0, "upnl_pct": 0, "liq_price": 0, "entry_price": 0,
        }

    @staticmethod
    def _to_stream_symbol(symbol: str) -> str:
        """Convert ASTERUSDT or ASTER/USDT:USDT to asterusdt for WS streams."""
        if ':' in symbol:
            symbol = symbol.split(':')[0]
        if '/' in symbol:
            parts = symbol.split('/')
            symbol = f"{parts[0]}{parts[1]}"
        return symbol.lower()

    def _get_listen_key(self) -> Optional[str]:
        """Get or refresh listenKey for private streams via REST."""
        try:
            session = requests.Session()
            session.headers.update({'X-MBX-APIKEY': self.api_key})

            # Generate signature
            params = {'timestamp': int(time.time() * 1000)}
            query = '&'.join(f"{k}={v}" for k, v in sorted(params.items()))
            sig = hmac.new(self.api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
            query += f"&signature={sig}"

            resp = session.post(f"{self.base_url}/fapi/v1/listenKey?{query}")
            if resp.status_code == 200:
                data = resp.json()
                key = data.get('listenKey')
                if key:
                    self._listen_key = key
                    self._listen_key_ts = time.time()
                    if self.logger:
                        self.logger.info(f"[WS-DATA] Got listenKey: {key[:8]}...")
                    return key
            if self.logger:
                self.logger.warning(f"[WS-DATA] listenKey request failed: {resp.status_code} {resp.text[:200]}")
        except Exception as e:
            if self.logger:
                self.logger.warning(f"[WS-DATA] listenKey error: {e}")
        return None

    def _keepalive_listen_key(self):
        """Extend listenKey validity (call every 30 min, expires at 60 min)."""
        if not self._listen_key:
            return
        try:
            session = requests.Session()
            session.headers.update({'X-MBX-APIKEY': self.api_key})
            params = {'listenKey': self._listen_key, 'timestamp': int(time.time() * 1000)}
            query = '&'.join(f"{k}={v}" for k, v in sorted(params.items()))
            sig = hmac.new(self.api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
            query += f"&signature={sig}"
            resp = session.put(f"{self.base_url}/fapi/v1/listenKey?{query}")
            if resp.status_code == 200:
                self._listen_key_ts = time.time()
        except Exception:
            pass

    def connect(self) -> bool:
        """Connect public + private WebSocket streams in background threads."""
        if not WEBSOCKET_AVAILABLE:
            if self.logger:
                self.logger.warning("[WS-DATA] websocket-client not installed")
            return False

        self._running = True

        pub_thread = threading.Thread(target=self._run_public, daemon=True)
        pub_thread.start()

        priv_thread = threading.Thread(target=self._run_private, daemon=True)
        priv_thread.start()

        # Wait briefly for connections
        for _ in range(30):  # 3 seconds max
            if self._connected_public:
                if self.logger:
                    self.logger.info(
                        f"[WS-DATA] Connected: public={self._connected_public} "
                        f"private={self._connected_private}"
                    )
                return True
            time.sleep(0.1)

        if self.logger:
            self.logger.warning(
                f"[WS-DATA] Partial connect: public={self._connected_public} "
                f"private={self._connected_private}"
            )
        return self._connected_public  # Public is essential

    def subscribe(self, symbol: str):
        """Subscribe to data streams for a symbol (any format accepted)."""
        stream_sym = self._to_stream_symbol(symbol)
        if stream_sym in self._subscribed_symbols:
            return

        self._subscribed_symbols.append(stream_sym)

        # Initialize caches
        self._orderbooks[stream_sym] = {'bids': [], 'asks': [], 'timestamp': 0}
        self._tickers[stream_sym] = {'last': 0.0}
        self._recent_trades[stream_sym] = []
        if stream_sym not in self._positions:
            self._positions[stream_sym] = {
                'long': dict(self._default_position_side),
                'short': dict(self._default_position_side),
            }

        if self._public_ws and self._connected_public:
            self._send_subscribe(self._public_ws, stream_sym)

    def _send_subscribe(self, ws, stream_sym: str):
        """Send subscribe message for a symbol's streams."""
        try:
            streams = [
                f"{stream_sym}@depth@100ms",
                f"{stream_sym}@aggTrade",
                f"{stream_sym}@bookTicker",
            ]
            sub_msg = {
                "method": "SUBSCRIBE",
                "params": streams,
                "id": self._sub_id,
            }
            self._sub_id += 1
            ws.send(json.dumps(sub_msg))
            if self.logger:
                self.logger.info(f"[WS-DATA] Subscribed public: {stream_sym}")
        except Exception as e:
            if self.logger:
                self.logger.warning(f"[WS-DATA] Public subscribe failed: {e}")

    def _run_public(self):
        """Public WS connection loop with reconnection."""
        backoff = 1.0
        while self._running:
            try:
                ws = websocket.create_connection(self.PUBLIC_URL, timeout=30)
                self._public_ws = ws
                self._connected_public = True
                backoff = 1.0

                if self.logger:
                    self.logger.info(f"[WS-DATA] Public connected: {self.PUBLIC_URL}")

                # Subscribe to all current symbols
                for stream_sym in self._subscribed_symbols:
                    self._send_subscribe(ws, stream_sym)

                # Receive loop
                last_ping = time.time()
                while self._running:
                    try:
                        msg = ws.recv()
                        if not msg:
                            continue

                        # Binance-style keepalive: pong is automatic via websocket-client
                        # but we send a pong frame response if we get a ping
                        now = time.time()
                        if now - last_ping > 180:  # Send ping every 3 min
                            ws.ping()
                            last_ping = now

                        data = json.loads(msg)

                        # Skip subscription confirmations
                        if 'result' in data and 'id' in data:
                            continue

                        # Route by event type (Binance stream format)
                        event_type = data.get('e', '')

                        if event_type == 'depthUpdate':
                            self._on_orderbook(data)
                        elif event_type == 'aggTrade':
                            self._on_agg_trade(data)
                        elif event_type == 'bookTicker':
                            self._on_book_ticker(data)

                    except websocket.WebSocketTimeoutException:
                        try:
                            ws.ping()
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
        """Private WS connection loop with listenKey auth."""
        backoff = 1.0
        while self._running:
            try:
                # Get listenKey
                listen_key = self._get_listen_key()
                if not listen_key:
                    if self.logger:
                        self.logger.warning(f"[WS-DATA] No listenKey, retry in {backoff}s")
                    time.sleep(backoff)
                    backoff = min(30.0, backoff * 2)
                    continue

                url = f"{self.PRIVATE_URL_BASE}{listen_key}"
                ws = websocket.create_connection(url, timeout=30)
                self._private_ws = ws
                self._connected_private = True
                backoff = 1.0

                if self.logger:
                    self.logger.info("[WS-DATA] Private connected (listenKey stream)")

                # Receive loop
                last_ping = time.time()
                last_keepalive = time.time()
                while self._running:
                    try:
                        msg = ws.recv()
                        if not msg:
                            continue

                        now = time.time()
                        if now - last_ping > 180:
                            ws.ping()
                            last_ping = now

                        # Refresh listenKey every 30 min
                        if now - last_keepalive > 1800:
                            self._keepalive_listen_key()
                            last_keepalive = now

                        data = json.loads(msg)
                        event_type = data.get('e', '')

                        if event_type == 'ORDER_TRADE_UPDATE':
                            self._on_order_update(data)
                        elif event_type == 'ACCOUNT_UPDATE':
                            self._on_account_update(data)

                    except websocket.WebSocketTimeoutException:
                        try:
                            ws.ping()
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

    # ── Message handlers ──

    def _on_orderbook(self, data: Dict):
        """Handle depthUpdate events (Binance format)."""
        symbol = data.get('s', '').lower()
        if not symbol:
            return

        bids = [[float(p), float(q)] for p, q in data.get('b', [])]
        asks = [[float(p), float(q)] for p, q in data.get('a', [])]

        ob = self._orderbooks.get(symbol)
        if not ob or not ob.get('bids'):
            # First update = treat as snapshot
            self._orderbooks[symbol] = {
                'bids': sorted(bids, key=lambda x: x[0], reverse=True),
                'asks': sorted(asks, key=lambda x: x[0]),
                'timestamp': time.time(),
            }
            return

        # Delta update: apply changes
        for price, qty in bids:
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

        for price, qty in asks:
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

    def _on_book_ticker(self, data: Dict):
        """Handle bookTicker events — best bid/ask (fastest ticker source)."""
        symbol = data.get('s', '').lower()
        if not symbol:
            return

        existing = self._tickers.get(symbol, {})
        bid = float(data.get('b', 0))
        ask = float(data.get('a', 0))
        if bid > 0:
            existing['bid'] = bid
        if ask > 0:
            existing['ask'] = ask
        if bid > 0 and ask > 0:
            existing['last'] = (bid + ask) / 2
        existing['timestamp'] = time.time()
        self._tickers[symbol] = existing

    def _on_agg_trade(self, data: Dict):
        """Handle aggTrade events."""
        symbol = data.get('s', '').lower()
        if not symbol:
            return

        trade_list = self._recent_trades.get(symbol, [])
        ts = data.get('T', 0)
        trade_list.append({
            'price': float(data.get('p', 0)),
            'qty': float(data.get('q', 0)),
            'side': 'sell' if data.get('m', False) else 'buy',  # m=True means buyer is maker → sell
            'timestamp': ts / 1000.0 if ts > 1e12 else float(ts),
        })

        if len(trade_list) > 500:
            trade_list = trade_list[-500:]
        self._recent_trades[symbol] = trade_list

    def _on_account_update(self, data: Dict):
        """Handle ACCOUNT_UPDATE — position changes."""
        account = data.get('a', {})
        positions = account.get('P', [])

        for pos in positions:
            symbol = pos.get('s', '').lower()
            if not symbol:
                continue

            pos_side = pos.get('ps', '').upper()  # LONG / SHORT / BOTH
            qty = float(pos.get('pa', 0))  # position amount
            entry_price = float(pos.get('ep', 0))
            upnl = float(pos.get('up', 0))

            if pos_side == 'LONG':
                side_key = 'long'
            elif pos_side == 'SHORT':
                side_key = 'short'
            elif pos_side == 'BOTH':
                side_key = 'long' if qty >= 0 else 'short'
            else:
                continue

            if symbol not in self._positions:
                self._positions[symbol] = {
                    'long': dict(self._default_position_side),
                    'short': dict(self._default_position_side),
                }

            self._positions[symbol][side_key] = {
                'qty': abs(qty),
                'price': entry_price,
                'realised': 0,
                'cum_realised': 0,
                'upnl': round(upnl, 4),
                'upnl_pct': 0,
                'liq_price': 0,
                'entry_price': entry_price,
            }

    def _on_order_update(self, data: Dict):
        """Handle ORDER_TRADE_UPDATE — log fills for awareness."""
        if self.logger:
            order = data.get('o', {})
            exec_type = order.get('x', '')  # TRADE, NEW, CANCELED, etc.
            if exec_type == 'TRADE':
                self.logger.info(
                    f"[WS-EXEC] {order.get('s')} {order.get('S')} "
                    f"qty={order.get('l', order.get('q'))} "
                    f"@ {order.get('L', order.get('p'))} "
                    f"exec={exec_type}"
                )

    # ── Cache getters with staleness check ──

    def get_orderbook(self, symbol: str) -> Optional[Dict]:
        """Get cached orderbook. Returns None if stale >5s."""
        stream_sym = self._to_stream_symbol(symbol)
        ob = self._orderbooks.get(stream_sym)
        if ob and ob.get('bids') and ob.get('asks'):
            if time.time() - ob.get('timestamp', 0) < 5.0:
                return ob
        return None

    def get_ticker(self, symbol: str) -> Optional[Dict]:
        """Get cached ticker. Returns None if stale >5s."""
        stream_sym = self._to_stream_symbol(symbol)
        tick = self._tickers.get(stream_sym)
        if tick and tick.get('last', 0) > 0:
            if time.time() - tick.get('timestamp', 0) < 5.0:
                return tick
        return None

    def get_positions(self, symbol: str) -> Optional[Dict]:
        """Get cached positions. Returns None if never received."""
        stream_sym = self._to_stream_symbol(symbol)
        return self._positions.get(stream_sym)

    def get_recent_trades(self, symbol: str) -> Optional[list]:
        """Get cached recent trades. Returns None if empty."""
        stream_sym = self._to_stream_symbol(symbol)
        trades = self._recent_trades.get(stream_sym)
        if trades:
            return trades
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


class AsterExchange(BaseExchange):
    """Aster exchange implementation — full strategy compatibility with Bybit adapter"""

    id = 'aster'  # Exchange identifier

    def __init__(self, config: Dict, logger=None):
        super().__init__(config, logger)
        self.id = 'aster'
        self.base_url = "https://fapi.asterdex.com"
        self.rate_limit_delay = 0.1  # 100ms between requests
        self.api_key = None
        self.api_secret = None
        self.session = None

        # Cached exchange info (avoid repeated /exchangeInfo calls)
        self._exchange_info_cache = None
        self._exchange_info_ts = 0
        self._exchange_info_ttl = 300  # 5 minute cache

        # Market precision cache: symbol -> {tick_size, step_size, min_qty, min_notional}
        self._precision_cache: Dict[str, Dict] = {}

        # Sibling dicts the vortex strategy reads directly (mirrors Bybit /
        # Hyperliquid / TxFlow / BloFin). _quantize_for_exchange in
        # strategies/vortex/main.py looks at self.exchange._market_qty_steps
        # and falls back to a 1.0 default on lookup miss — without these,
        # every Aster symbol with a sub-1 step (most of them) over-rounds.
        # Populated in _load_exchange_info().
        self._market_qty_steps: Dict[str, float] = {}
        self._market_tick_sizes: Dict[str, float] = {}
        self._market_min_order_values: Dict[str, float] = {}

        # Strategy flags inspected by the bot. Aster is BINANCE-shaped:
        # hedge-mode supported (one-way OR hedge depending on account setting),
        # treated as hedge / dual-side like Bybit. Vortex passes position_side
        # explicitly via _place_order_with_position_side.
        self.is_netting_mode = False
        self.batch_cancel_and_place = False

        # Order registry for deduplication (mirrors Bybit's _order_registry)
        self._order_registry: Dict[str, Dict] = {}  # orderLinkId -> {side, price, qty, order_id, placed_at}
        self._order_lock = threading.Lock()

        # WebSocket data streams (real-time market data, 0ms reads)
        self.websocket_data_enabled = config.get('websocket_data', False)
        self._ws_data: Optional[AsterWebSocketDataManager] = None
        self.websocket_orders_enabled = False

        if self.websocket_data_enabled and not WEBSOCKET_AVAILABLE:
            self.logger.warning("websocket_data enabled but websocket-client not installed — falling back to REST")
            self.websocket_data_enabled = False

    # ========================================
    # CONNECTION & SETUP
    # ========================================

    def connect(self) -> bool:
        """Connect to Aster"""
        try:
            self.api_key = self.config['api_key']
            self.api_secret = self.config['api_secret']

            # Create session
            self.session = requests.Session()
            self.session.headers.update({
                'X-MBX-APIKEY': self.api_key
            })

            # Test connection by fetching account info
            response = self._signed_request('GET', '/fapi/v1/account')
            if response:
                self.logger.info("Successfully connected to Aster")
                # Pre-load exchange info and markets
                self._load_exchange_info()
                self.load_markets()

                # WebSocket data streams (real-time market data, 0ms reads)
                if self.websocket_data_enabled:
                    self._ws_data = AsterWebSocketDataManager(
                        api_key=self.api_key,
                        api_secret=self.api_secret,
                        base_url=self.base_url,
                        testnet=self.config.get('testnet', False),
                        logger=self.logger,
                    )
                    if self._ws_data.connect():
                        self.logger.info("WebSocket DATA streams ENABLED (orderbook, trades, tickers, positions)")
                    else:
                        self.logger.warning("WebSocket data streams slow to connect — reconnecting in background, REST fallback active")
                    self.rate_limit_delay = 0.01  # Reduce REST rate limit when WS configured

                return True
            return False

        except Exception as e:
            self.logger.error(f"Failed to connect to Aster: {e}")
            return False

    def setup_hedge_mode(self) -> bool:
        """Setup hedge position mode"""
        try:
            current_mode = self._signed_request('GET', '/fapi/v1/positionSide/dual')
            if current_mode and current_mode.get('dualSidePosition') is True:
                self.logger.info("Hedge mode already enabled")
                return True

            response = self._signed_request('POST', '/fapi/v1/positionSide/dual', {
                'dualSidePosition': 'true'
            })
            if response:
                self.logger.info("Set hedge position mode")
                return True
            return False
        except Exception as e:
            self.logger.info(f"Hedge mode setup: {e}")
            return True  # May already be enabled

    def set_leverage(self, symbol: str, leverage: int) -> bool:
        """Set leverage for symbol. Signature matches Bybit: (symbol, leverage)"""
        try:
            symbol_formatted = self.convert_symbol_format(symbol)
            self._rate_limit()

            response = self._signed_request('POST', '/fapi/v1/leverage', {
                'symbol': symbol_formatted,
                'leverage': str(leverage)
            })

            if response:
                self.logger.info(f"Set leverage to {leverage}x for {symbol}")
                return True
            return False
        except Exception as e:
            self.logger.info(f"Leverage setting: {e}")
            return True  # May already be set

    # ========================================
    # LOW-LEVEL API HELPERS
    # ========================================

    def _rate_limit(self):
        """Apply rate limiting"""
        time.sleep(self.rate_limit_delay)

    def _generate_signature(self, query_string: str) -> str:
        """Generate signature using HMAC SHA256"""
        try:
            return hmac.new(
                self.api_secret.encode('utf-8'),
                query_string.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()
        except Exception as e:
            self.logger.error(f"Error generating signature: {e}")
            return ""

    def _signed_request(self, method: str, endpoint: str, params: Dict = None) -> Dict:
        """Make signed API request"""
        # No broker/affiliate ID for Aster
        try:
            if params is None:
                params = {}

            params['timestamp'] = int(time.time() * 1000)

            sorted_params = sorted(params.items())
            query_string = '&'.join([f"{k}={v}" for k, v in sorted_params])

            signature = self._generate_signature(query_string)
            query_string_with_sig = f"{query_string}&signature={signature}"
            url = f"{self.base_url}{endpoint}?{query_string_with_sig}"

            if method == 'GET':
                response = self.session.get(url)
            elif method == 'POST':
                response = self.session.post(url)
            elif method == 'PUT':
                response = self.session.put(url)
            elif method == 'DELETE':
                response = self.session.delete(url)
            else:
                raise ValueError(f"Unsupported method: {method}")

            response.raise_for_status()
            return response.json()

        except Exception as e:
            self.logger.error(f"API request error [{method} {endpoint}]: {e}")
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_detail = e.response.json()
                    self.logger.error(f"Error detail: {error_detail}")
                except Exception:
                    self.logger.error(f"Error response: {e.response.text}")
            return {}

    def _public_request(self, endpoint: str, params: Dict = None) -> Dict:
        """Make unsigned public API request"""
        try:
            url = f"{self.base_url}{endpoint}"
            response = requests.get(url, params=params or {})
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.logger.error(f"Public API error [{endpoint}]: {e}")
            return {}

    def convert_symbol_format(self, symbol: str) -> str:
        """Convert any symbol format to Aster's BTCUSDT format"""
        # DOGE/USDT:USDT -> DOGEUSDT
        if ':' in symbol:
            symbol = symbol.split(':')[0]
        if '/' in symbol:
            parts = symbol.split('/')
            return f"{parts[0]}{parts[1]}"
        return symbol

    # ========================================
    # EXCHANGE INFO & PRECISION (CACHED)
    # ========================================

    def _load_exchange_info(self):
        """Load and cache exchange info"""
        now = time.time()
        if self._exchange_info_cache and (now - self._exchange_info_ts) < self._exchange_info_ttl:
            return self._exchange_info_cache

        try:
            data = self._public_request('/fapi/v1/exchangeInfo')
            if data and 'symbols' in data:
                self._exchange_info_cache = data
                self._exchange_info_ts = now

                # Build precision cache
                for sym_info in data['symbols']:
                    sym = sym_info.get('symbol', '')
                    tick_size = 0.00001
                    step_size = 0.001
                    min_qty = 0.001
                    min_notional = 5.0

                    for f in sym_info.get('filters', []):
                        if f.get('filterType') == 'PRICE_FILTER':
                            tick_size = float(f.get('tickSize', tick_size))
                        elif f.get('filterType') == 'LOT_SIZE':
                            step_size = float(f.get('stepSize', step_size))
                            min_qty = float(f.get('minQty', min_qty))
                        elif f.get('filterType') == 'MIN_NOTIONAL':
                            min_notional = float(f.get('notional', min_notional))

                    self._precision_cache[sym] = {
                        'tick_size': tick_size,
                        'step_size': step_size,
                        'min_qty': min_qty,
                        'min_notional': min_notional,
                    }
                    # Also publish to the three sibling maps the vortex
                    # strategy reads. Aster's filter values are already in
                    # TOKENS (no contractSize translation needed, unlike
                    # BloFin), so step_size + tick_size + min_notional drop
                    # straight in.
                    if step_size > 0:
                        self._market_qty_steps[sym] = step_size
                    if tick_size > 0:
                        self._market_tick_sizes[sym] = tick_size
                    if min_notional > 0:
                        self._market_min_order_values[sym] = min_notional

                self.logger.info(
                    f"Cached precision for {len(self._precision_cache)} Aster symbols "
                    f"(qty_steps={len(self._market_qty_steps)}, "
                    f"tick_sizes={len(self._market_tick_sizes)}, "
                    f"min_notionals={len(self._market_min_order_values)})"
                )
                # Sanity sample for symbols where step-default-1.0 would have
                # been wrong.
                for sample in ("BTCUSDT", "ETHUSDT", "LINKUSDT", "ASTERUSDT"):
                    step = self._market_qty_steps.get(sample)
                    if step is not None:
                        self.logger.info(
                            f"  {sample}: step={step} tick={self._market_tick_sizes.get(sample)} "
                            f"min_notional={self._market_min_order_values.get(sample)}"
                        )
            return data
        except Exception as e:
            self.logger.error(f"Error loading exchange info: {e}")
            return {}

    def get_symbol_info(self, symbol: str) -> Dict:
        """Get symbol trading information"""
        try:
            symbol = self.convert_symbol_format(symbol)
            info = self._load_exchange_info()
            if info and 'symbols' in info:
                for sym_info in info['symbols']:
                    if sym_info.get('symbol') == symbol:
                        return sym_info
            return {}
        except Exception as e:
            self.logger.error(f"Error getting symbol info for {symbol}: {e}")
            return {}

    def get_precision_and_limits(self, symbol: str):
        """Get precision and limits for symbol — uses cache"""
        try:
            sym = self.convert_symbol_format(symbol)
            if sym not in self._precision_cache:
                self._load_exchange_info()

            prec = self._precision_cache.get(sym)
            if prec:
                return prec['step_size'], prec['tick_size'], prec['min_qty']
            return None, None, None
        except Exception as e:
            self.logger.error(f"Error getting precision for {symbol}: {e}")
            return None, None, None

    def get_exchange_rules(self, symbol: str) -> dict:
        """Get exchange constraints for order validation (non-CCXT override)."""
        try:
            sym = self.convert_symbol_format(symbol)
            if sym not in self._precision_cache:
                self._load_exchange_info()

            prec = self._precision_cache.get(sym, {})
            return {
                'min_order_value': prec.get('min_notional', 5.0),
                'min_qty': prec.get('min_qty', 1.0),
                'qty_step': prec.get('step_size', 1.0),
                'tick_size': prec.get('tick_size', 0.0001),
            }
        except Exception as e:
            self.logger.error(f"get_exchange_rules({symbol}): {e}")
            return {'min_order_value': 5.0, 'min_qty': 1.0, 'qty_step': 1.0, 'tick_size': 0.0001}

    def _snap_price(self, symbol: str, price: float) -> float:
        """Snap price to tick size"""
        sym = self.convert_symbol_format(symbol)
        prec = self._precision_cache.get(sym)
        if prec and prec['tick_size'] > 0:
            tick = prec['tick_size']
            return round(round(price / tick) * tick, 10)
        return price

    def _snap_qty(self, symbol: str, qty: float) -> float:
        """Snap quantity to step size"""
        sym = self.convert_symbol_format(symbol)
        prec = self._precision_cache.get(sym)
        if prec and prec['step_size'] > 0:
            step = prec['step_size']
            return round(round(qty / step) * step, 8)
        return qty

    # ========================================
    # MARKET DATA — REST
    # ========================================

    def get_balance(self) -> float:
        """Get USDT balance"""
        try:
            self._rate_limit()
            response = self._signed_request('GET', '/fapi/v1/account')

            if response:
                if 'totalWalletBalance' in response:
                    return float(response.get('totalWalletBalance', 0.0))

                if 'assets' in response and isinstance(response['assets'], list):
                    for asset in response['assets']:
                        if asset.get('asset') == 'USDT':
                            balance = asset.get('walletBalance') or asset.get('availableBalance') or asset.get('balance', 0.0)
                            return float(balance)
            return 0.0
        except Exception as e:
            self.logger.error(f"Error fetching balance: {e}")
            return 0.0

    def get_current_price(self, symbol: str) -> float:
        """Get current price for symbol"""
        try:
            self._rate_limit()
            symbol_fmt = self.convert_symbol_format(symbol)
            data = self._public_request('/fapi/v1/ticker/price', {'symbol': symbol_fmt})
            return float(data.get('price', 0.0))
        except Exception as e:
            self.logger.error(f"Error fetching price for {symbol}: {e}")
            return 0.0

    def get_ticker(self, symbol: str) -> Dict:
        """Get ticker data for symbol"""
        try:
            self._rate_limit()
            symbol_fmt = self.convert_symbol_format(symbol)
            data = self._public_request('/fapi/v1/ticker/24hr', {'symbol': symbol_fmt})

            return {
                'last': float(data.get('lastPrice', 0.0)),
                'bid': float(data.get('bidPrice', 0.0)),
                'ask': float(data.get('askPrice', 0.0)),
                'high': float(data.get('highPrice', 0.0)),
                'low': float(data.get('lowPrice', 0.0)),
                'volume': float(data.get('volume', 0.0)),
                'quoteVolume': float(data.get('quoteVolume', 0.0)),
                'info': data
            }
        except Exception as e:
            self.logger.error(f"Error fetching ticker for {symbol}: {e}")
            return {'last': 0.0}

    def get_orderbook(self, symbol: str, limit: int = 50) -> Dict:
        """Get orderbook depth for symbol"""
        try:
            self._rate_limit()
            symbol_fmt = self.convert_symbol_format(symbol)
            limit = max(5, min(100, limit))

            data = self._public_request('/fapi/v1/depth', {'symbol': symbol_fmt, 'limit': limit})

            return {
                'bids': [[float(price), float(qty)] for price, qty in data.get('bids', [])],
                'asks': [[float(price), float(qty)] for price, qty in data.get('asks', [])]
            }
        except Exception as e:
            self.logger.error(f"Error fetching orderbook for {symbol}: {e}")
            return {'bids': [], 'asks': []}

    def get_klines(self, symbol: str, interval: str = '1m', limit: int = 20) -> List[Dict]:
        """Get OHLCV klines in dict format (matches Bybit's get_klines)."""
        try:
            self._rate_limit()
            symbol_fmt = self.convert_symbol_format(symbol)
            data = self._public_request('/fapi/v1/klines', {
                'symbol': symbol_fmt,
                'interval': interval,
                'limit': limit
            })

            if not isinstance(data, list):
                return []

            return [
                {
                    'timestamp': int(candle[0]),
                    'open': float(candle[1]),
                    'high': float(candle[2]),
                    'low': float(candle[3]),
                    'close': float(candle[4]),
                }
                for candle in data
            ]
        except Exception as e:
            self.logger.warning(f"Error fetching klines for {symbol}: {e}")
            return []

    def get_recent_trades(self, symbol: str, limit: int = 200) -> List[Dict]:
        """Get recent public trades in strategy format: {price, qty, side, timestamp}"""
        try:
            self._rate_limit()
            symbol_fmt = self.convert_symbol_format(symbol)
            data = self._public_request('/fapi/v1/trades', {'symbol': symbol_fmt, 'limit': limit})

            if not isinstance(data, list):
                return []

            trades = []
            for t in data:
                ts = t.get('time', 0)
                trades.append({
                    'price': float(t.get('price', 0)),
                    'qty': float(t.get('qty', 0)),
                    'side': 'sell' if t.get('isBuyerMaker') else 'buy',
                    'timestamp': ts / 1000.0 if ts > 1e12 else float(ts),
                })
            return trades
        except Exception as e:
            self.logger.warning(f"Error fetching recent trades for {symbol}: {e}")
            return []

    def get_fee_rate(self, symbol: str) -> Optional[Dict]:
        """Get account fee rate for a symbol. Returns {maker_fee_bps, taker_fee_bps} or None.

        Aster API may not expose fee rates directly. Falls back to config or defaults.
        """
        try:
            self._rate_limit()
            symbol_fmt = self.convert_symbol_format(symbol)
            response = self._signed_request('GET', '/fapi/v1/commissionRate', {'symbol': symbol_fmt})

            if response and 'makerCommissionRate' in response:
                maker = float(response.get('makerCommissionRate', 0))
                taker = float(response.get('takerCommissionRate', 0))
                return {
                    'maker_fee_bps': round(maker * 10000, 2),
                    'taker_fee_bps': round(taker * 10000, 2),
                }

            # Fallback: try account endpoint
            if response and 'makerCommission' in response:
                maker = float(response.get('makerCommission', 0))
                taker = float(response.get('takerCommission', 0))
                return {
                    'maker_fee_bps': round(maker, 2),
                    'taker_fee_bps': round(taker, 2),
                }

            return None
        except Exception as e:
            self.logger.warning(f"Error fetching fee rate for {symbol}: {e}")
            return None

    # ========================================
    # CACHED DATA METHODS (REST fallback — no WS yet)
    # Strategies call *_cached first; these just call REST.
    # ========================================

    def subscribe_data(self, symbol: str):
        """Subscribe to real-time WS data streams for a symbol."""
        if self._ws_data:
            self._ws_data.subscribe(symbol)

    def get_current_price_cached(self, symbol: str) -> float:
        """Get price — WS cache with REST fallback."""
        if self._ws_data:
            tick = self._ws_data.get_ticker(symbol)
            if tick and tick.get('last', 0) > 0:
                return tick['last']
        return self.get_current_price(symbol)

    def get_orderbook_cached(self, symbol: str, limit: int = 50) -> Dict:
        """Get orderbook — WS cache with REST fallback."""
        if self._ws_data:
            ob = self._ws_data.get_orderbook(symbol)
            if ob:
                return ob
        return self.get_orderbook(symbol, limit)

    def get_positions_cached(self, symbol: str) -> Dict:
        """Get positions — WS cache with REST fallback."""
        if self._ws_data:
            pos = self._ws_data.get_positions(symbol)
            if pos:
                return pos
        return self.get_positions(symbol)

    def get_recent_trades_cached(self, symbol: str, limit: int = 200) -> List[Dict]:
        """Get recent trades — WS cache with REST fallback."""
        if self._ws_data:
            trades = self._ws_data.get_recent_trades(symbol)
            if trades:
                return trades
        return self.get_recent_trades(symbol, limit)

    # ========================================
    # POSITIONS
    # ========================================

    def get_positions(self, symbol: str) -> dict:
        """Get positions for symbol — matches Bybit format"""
        values = {
            "long": {
                "qty": 0.0, "price": 0.0, "realised": 0, "cum_realised": 0,
                "upnl": 0, "upnl_pct": 0, "liq_price": 0, "entry_price": 0,
            },
            "short": {
                "qty": 0.0, "price": 0.0, "realised": 0, "cum_realised": 0,
                "upnl": 0, "upnl_pct": 0, "liq_price": 0, "entry_price": 0,
            },
        }

        try:
            self._rate_limit()
            symbol_formatted = self.convert_symbol_format(symbol)
            response = self._signed_request('GET', '/fapi/v1/account', {})

            if response and 'positions' in response:
                for pos in response['positions']:
                    if pos.get('symbol') != symbol_formatted:
                        continue

                    position_side = pos.get('positionSide', '').upper()
                    if position_side == 'LONG':
                        side_key = 'long'
                    elif position_side == 'SHORT':
                        side_key = 'short'
                    elif position_side == 'BOTH':
                        qty = float(pos.get('positionAmt', 0))
                        if qty > 0:
                            side_key = 'long'
                        elif qty < 0:
                            side_key = 'short'
                        else:
                            continue
                    else:
                        continue

                    qty = float(pos.get('positionAmt', 0))
                    entry_price = float(pos.get('entryPrice', 0))
                    unrealized_pnl = float(pos.get('unrealizedProfit', 0))
                    liq_price = float(pos.get('liquidationPrice', 0))

                    values[side_key]["qty"] = abs(qty)
                    values[side_key]["price"] = entry_price
                    values[side_key]["entry_price"] = entry_price
                    values[side_key]["upnl"] = round(unrealized_pnl, 4)
                    values[side_key]["liq_price"] = liq_price

                    if entry_price > 0 and abs(qty) > 0:
                        position_value = entry_price * abs(qty)
                        values[side_key]["upnl_pct"] = round((unrealized_pnl / position_value) * 100, 4)

            self.logger.info(f"Positions for {symbol}: Long={values['long']['qty']}, Short={values['short']['qty']}")
        except Exception as e:
            self.logger.error(f"Error getting positions for {symbol}: {e}")

        return values

    # ========================================
    # ORDER MANAGEMENT
    # ========================================

    def get_open_orders(self, symbol: str = None) -> List[Dict]:
        """Get open orders"""
        try:
            self._rate_limit()
            params = {}
            if symbol:
                params['symbol'] = self.convert_symbol_format(symbol)

            response = self._signed_request('GET', '/fapi/v1/openOrders', params)

            formatted_orders = []
            if isinstance(response, list):
                for order in response:
                    formatted_orders.append({
                        'id': str(order.get('orderId', '')),
                        'clientOrderId': order.get('clientOrderId', ''),
                        'symbol': order.get('symbol', ''),
                        'side': order.get('side', '').lower(),
                        'amount': float(order.get('origQty', 0)),
                        'price': float(order.get('price', 0)),
                        'type': order.get('type', '').lower(),
                        'reduce_only': order.get('reduceOnly', False),
                        'status': order.get('status', '').lower(),
                        'positionSide': order.get('positionSide', '')
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
        position_idx: int = 0,
        order_link_id: str = None,
    ) -> Dict:
        """Place an order. Signature matches Bybit adapter for full strategy compatibility."""
        try:
            symbol_fmt = self.convert_symbol_format(symbol)

            # Snap to precision
            step_size, tick_size, min_amount = self.get_precision_and_limits(symbol_fmt)
            if step_size is None:
                self.logger.error(f"Could not get precision for {symbol}")
                return {}

            amount = round(round(amount / step_size) * step_size, 8)
            if price and order_type.lower() == 'limit':
                price = round(round(price / tick_size) * tick_size, 8)

            if min_amount and amount < min_amount:
                self.logger.error(f"Order quantity {amount} below minimum {min_amount}")
                return {}

            self._rate_limit()

            # Setup hedge mode once
            if not hasattr(self, '_hedge_mode_set'):
                self.setup_hedge_mode()
                self._hedge_mode_set = True

            # Map position_idx to positionSide (Bybit uses idx, Aster uses side string)
            if position_idx == 1:
                pos_side = 'LONG'
            elif position_idx == 2:
                pos_side = 'SHORT'
            else:
                pos_side = 'LONG' if side.lower() == 'buy' else 'SHORT'

            params = {
                'symbol': symbol_fmt,
                'side': side.upper(),
                'positionSide': pos_side,
                'type': order_type.upper(),
                'quantity': str(amount),
            }

            # timeInForce is valid ONLY for LIMIT orders — sending it on a
            # MARKET order is rejected by Aster (-1106 "Parameter 'timeInForce'
            # sent when not required"), which silently broke every market close
            # (fast-exit / emergency). Add timeInForce + price for limits only.
            if order_type.lower() == 'limit':
                params['timeInForce'] = 'GTX' if post_only else 'GTC'
                if price:
                    params['price'] = str(price)

            if order_link_id:
                params['newClientOrderId'] = order_link_id

            # Remove None values
            params = {k: v for k, v in params.items() if v is not None}

            response = self._signed_request('POST', '/fapi/v1/order', params)

            if response and 'orderId' in response:
                result_id = str(response['orderId'])
                self.logger.info(f"Placed {side} order: {symbol_fmt} {amount} @ {price}")

                # Register in order registry
                if order_link_id:
                    with self._order_lock:
                        self._order_registry[order_link_id] = {
                            'side': side.lower(),
                            'price': price,
                            'qty': amount,
                            'order_id': result_id,
                            'placed_at': time.time(),
                        }

                return {
                    'id': result_id,
                    'clientOrderId': order_link_id or response.get('clientOrderId', ''),
                    'symbol': symbol,
                    'side': side,
                    'amount': amount,
                    'price': price,
                    'status': response.get('status', '').lower()
                }

            return {}

        except Exception as e:
            self.logger.error(f"Error placing order: {e}")
            return {}

    def cancel_order(self, order_id: str = None, symbol: str = None, order_link_id: str = None) -> bool:
        """Cancel specific order. Supports both order_id and order_link_id (matches Bybit)."""
        try:
            self._rate_limit()
            symbol_fmt = self.convert_symbol_format(symbol) if symbol else ''

            params = {'symbol': symbol_fmt}
            if order_link_id:
                params['origClientOrderId'] = order_link_id
            elif order_id:
                params['orderId'] = order_id
            else:
                self.logger.error("cancel_order: need either order_id or order_link_id")
                return False

            response = self._signed_request('DELETE', '/fapi/v1/order', params)

            if response:
                id_str = order_link_id or order_id
                self.logger.info(f"Cancelled order {id_str} for {symbol}")
                # Remove from registry
                if order_link_id:
                    with self._order_lock:
                        self._order_registry.pop(order_link_id, None)
                return True
            return False
        except Exception as e:
            id_str = order_link_id or order_id
            self.logger.error(f"Error cancelling order {id_str}: {e}")
            return False

    def cancel_all_orders(self, symbol: str = None) -> bool:
        """Cancel all orders for symbol or all symbols"""
        try:
            if symbol:
                self._rate_limit()
                symbol_fmt = self.convert_symbol_format(symbol)
                response = self._signed_request('DELETE', '/fapi/v1/allOpenOrders', {
                    'symbol': symbol_fmt
                })
                if response:
                    self.logger.info(f"Cancelled all orders for {symbol}")
                    return True
                return False
            else:
                orders = self.get_open_orders()
                symbols = set(order['symbol'] for order in orders)
                return all(self.cancel_all_orders(sym) for sym in symbols)
        except Exception as e:
            self.logger.error(f"Error cancelling all orders: {e}")
            return False

    def amend_order(
        self,
        symbol: str,
        order_id: str = None,
        order_link_id: str = None,
        qty: float = None,
        price: float = None,
    ) -> bool:
        """Amend an existing order's price/qty.

        Aster (Binance-style) doesn't have native amend — cancel+replace.
        Returns True on success.
        """
        if not order_id and not order_link_id:
            self.logger.error("[AMEND] Need either order_id or order_link_id")
            return False
        if qty is None and price is None:
            self.logger.error("[AMEND] Need at least qty or price to amend")
            return False

        try:
            # Get existing order info from registry
            existing = None
            if order_link_id:
                with self._order_lock:
                    existing = self._order_registry.get(order_link_id)

            # Cancel existing
            cancelled = self.cancel_order(order_id=order_id, symbol=symbol, order_link_id=order_link_id)
            if not cancelled:
                self.logger.warning(f"[AMEND] Cancel failed for {order_link_id or order_id}")
                return False

            # Re-place with new params
            if existing:
                new_qty = qty if qty is not None else existing.get('qty', 0)
                new_price = price if price is not None else existing.get('price', 0)
                new_side = existing.get('side', 'buy')

                result = self.place_order(
                    symbol=symbol,
                    side=new_side,
                    amount=new_qty,
                    price=new_price,
                    order_type='limit',
                    post_only=True,
                    order_link_id=order_link_id,
                )
                if result and result.get('id'):
                    self.logger.info(f"[AMEND] {order_link_id or order_id} → price={new_price} qty={new_qty}")
                    return True

            self.logger.warning(f"[AMEND] Re-place failed for {order_link_id or order_id}")
            return False
        except Exception as e:
            self.logger.error(f"[AMEND] Error: {e}")
            return False

    def place_orders_batch(self, orders: List[Dict], symbol: str, position_idx: int = 0) -> List[Dict]:
        """Place multiple orders. Sequential fallback (Aster has no batch API).

        Args match Bybit's batch interface for strategy compatibility.
        """
        results = []
        for order in orders:
            side = order.get('side', 'buy')
            amount = order.get('amount', 0)
            price = order.get('price')
            link_id = order.get('order_link_id')
            reduce_only = order.get('reduce_only', False)
            post_only = order.get('post_only', True)

            result = self.place_order(
                symbol=symbol,
                side=side,
                amount=amount,
                price=price,
                order_type='limit',
                reduce_only=reduce_only,
                post_only=post_only,
                position_idx=position_idx,
                order_link_id=link_id,
            )

            if result and result.get('id'):
                results.append({
                    'id': result['id'],
                    'clientOrderId': link_id or '',
                    'symbol': symbol,
                    'status': 'open',
                    'success': True,
                })
            else:
                results.append({
                    'clientOrderId': link_id or '',
                    'symbol': symbol,
                    'status': 'failed',
                    'success': False,
                })

        success_count = sum(1 for r in results if r.get('success'))
        self.logger.info(f"[BATCH] {success_count}/{len(orders)} orders placed for {symbol}")
        return results

    def cancel_orders_batch(self, orders: List[Dict], symbol: str) -> List[bool]:
        """Cancel multiple orders. Sequential fallback (Aster has no batch API).

        Args match Bybit's batch interface for strategy compatibility.
        """
        results = []
        for order in orders:
            link_id = order.get('order_link_id')
            oid = order.get('order_id')
            success = self.cancel_order(order_id=oid, symbol=symbol, order_link_id=link_id)
            results.append(success)

        success_count = sum(results)
        self.logger.info(f"[BATCH-CANCEL] {success_count}/{len(orders)} cancelled for {symbol}")
        return results

    # ========================================
    # ORDER REGISTRY (matches Bybit)
    # ========================================

    def _generate_order_link_id(self, symbol: str, side: str) -> str:
        """Generate unique client order ID for idempotency. Matches Bybit format."""
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        symbol_clean = self.convert_symbol_format(symbol)
        return f"{side[0].upper()}_{symbol_clean}_{timestamp}_{unique_id}"

    def get_registered_orders(self) -> Dict[str, Dict]:
        """Get all registered orders. Returns orderLinkId -> order info."""
        with self._order_lock:
            return dict(self._order_registry)

    def unregister_order(self, order_link_id: str) -> bool:
        """Remove order from registry."""
        with self._order_lock:
            return self._order_registry.pop(order_link_id, None) is not None

    def has_order_at_price(self, side: str, price: float, tolerance_bps: float = 1.0) -> bool:
        """Check if we already have an order at/near this price."""
        with self._order_lock:
            for link_id, info in self._order_registry.items():
                if info.get('side') != side.lower():
                    continue
                existing_price = info.get('price', 0)
                if existing_price > 0:
                    diff_bps = abs(existing_price - price) / existing_price * 10000
                    if diff_bps <= tolerance_bps:
                        return True
        return False

    # ========================================
    # INTERNAL ORDER HELPERS
    # ========================================

    def _place_order_with_position_side(
        self,
        symbol: str,
        side: str,
        amount: float,
        price: float,
        order_type: str = "limit",
        reduce_only: bool = False,
        position_side: str = None,
        post_only: bool = False,
    ) -> Dict:
        """Place an order with explicit position side control"""
        # Map position_side string to position_idx
        if position_side:
            pos_idx = 1 if position_side.lower() == 'long' else 2
        else:
            pos_idx = 0  # Auto-detect

        return self.place_order(
            symbol=symbol,
            side=side,
            amount=amount,
            price=price,
            order_type=order_type,
            reduce_only=reduce_only,
            post_only=post_only,
            position_idx=pos_idx,
        )

    def place_take_profit_order(self, symbol: str, side: str, amount: float, price: float, position_side: str = None) -> Dict:
        """Place a take profit (reduce-only) order"""
        return self._place_order_with_position_side(
            symbol=symbol, side=side, amount=amount, price=price,
            order_type="limit", reduce_only=True, position_side=position_side,
        )

    def _place_market_order(self, symbol: str, side: str, amount: float, position_side: str) -> Dict:
        """Place market order helper"""
        pos_idx = 1 if position_side.lower() == 'long' else 2
        return self.place_order(
            symbol=symbol, side=side, amount=amount, price=None,
            order_type='market', position_idx=pos_idx,
        )

    # ========================================
    # CCXT-STYLE WRAPPER METHODS
    # Compatibility layer for strategies that call CCXT methods directly
    # ========================================

    def load_markets(self) -> Dict:
        """Load markets info — CCXT style"""
        try:
            info = self._load_exchange_info()
            self.markets = {}
            if info and 'symbols' in info:
                for sym_info in info['symbols']:
                    sym = sym_info.get('symbol')
                    base = sym_info.get('baseAsset', '')
                    quote = sym_info.get('quoteAsset', '')
                    ccxt_symbol = f"{base}/{quote}:{quote}"

                    # Extract precision for CCXT-compatible format
                    prec = self._precision_cache.get(sym, {})

                    self.markets[ccxt_symbol] = {
                        'id': sym,
                        'symbol': ccxt_symbol,
                        'base': base,
                        'quote': quote,
                        'settle': quote,
                        'precision': {
                            'price': prec.get('tick_size', 0.0001),
                            'amount': prec.get('step_size', 1.0),
                        },
                        'limits': {
                            'amount': {'min': prec.get('min_qty', 1.0)},
                            'cost': {'min': prec.get('min_notional', 5.0)},
                        },
                        'info': sym_info,
                    }

            self.logger.info(f"Loaded {len(self.markets)} markets from Aster")
            return self.markets
        except Exception as e:
            self.logger.error(f"Error loading markets: {e}")
            return {}

    def fetch_ticker(self, symbol: str, params: dict = None) -> Dict:
        """Fetch ticker — CCXT style"""
        return self.get_ticker(symbol)

    def fetch_order_book(self, symbol: str, limit: int = 50, params: dict = None) -> Dict:
        """Fetch order book — CCXT style"""
        return self.get_orderbook(symbol, limit)

    def fetch_open_orders(self, symbol: str = None, since: int = None, limit: int = None, params: dict = None) -> List[Dict]:
        """Fetch open orders — CCXT style"""
        return self.get_open_orders(symbol)

    def fetch_positions(self, symbols: List[str] = None, params: dict = None) -> List[Dict]:
        """Fetch positions — CCXT style (returns list format)"""
        positions = []
        try:
            self._rate_limit()
            response = self._signed_request('GET', '/fapi/v1/account', {})

            if response and 'positions' in response:
                for pos in response['positions']:
                    qty = float(pos.get('positionAmt', 0))
                    if qty == 0:
                        continue

                    symbol_raw = pos.get('symbol', '')
                    position_side = pos.get('positionSide', 'BOTH')

                    if symbols:
                        matched = any(self.convert_symbol_format(s) == symbol_raw for s in symbols)
                        if not matched:
                            continue

                    entry_price = float(pos.get('entryPrice', 0))
                    unrealized_pnl = float(pos.get('unrealizedProfit', 0))
                    liq_price = float(pos.get('liquidationPrice', 0))

                    ccxt_symbol = symbol_raw
                    if symbol_raw.endswith('USDT'):
                        base = symbol_raw[:-4]
                        ccxt_symbol = f"{base}/USDT:USDT"

                    if position_side.upper() == 'LONG':
                        side = 'long'
                    elif position_side.upper() == 'SHORT':
                        side = 'short'
                    else:
                        side = 'long' if qty > 0 else 'short'

                    positions.append({
                        'symbol': ccxt_symbol,
                        'side': side,
                        'contracts': abs(qty),
                        'contractSize': 1,
                        'entryPrice': entry_price,
                        'unrealizedPnl': unrealized_pnl,
                        'liquidationPrice': liq_price,
                        'percentage': 0,  # Not available directly
                        'info': pos,
                    })

            return positions
        except Exception as e:
            self.logger.error(f"Error fetching positions: {e}")
            return []

    def fetch_positions_cached(self, symbols: list = None) -> list:
        """CCXT-compatible fetch_positions — REST fallback (no WS cache)."""
        return self.fetch_positions(symbols)

    def fetch_order_book_cached(self, symbol: str, limit: int = 50) -> Dict:
        """CCXT-compatible fetch_order_book — REST fallback."""
        return self.get_orderbook(symbol, limit)

    def fetch_trades_cached(self, symbol: str, since=None, limit: int = 50, params=None) -> list:
        """CCXT-compatible fetch_trades — REST fallback."""
        return self.fetch_trades(symbol, since, limit, params)

    def fetch_ohlcv(self, symbol: str, timeframe: str = '15m', limit: int = 100) -> list:
        """Fetch OHLCV (candlestick) data — CCXT format [[ts, o, h, l, c, v], ...]"""
        try:
            self._rate_limit()
            symbol_fmt = self.convert_symbol_format(symbol)
            data = self._public_request('/fapi/v1/klines', {
                'symbol': symbol_fmt,
                'interval': timeframe,
                'limit': limit,
            })

            if not isinstance(data, list):
                return []

            return [
                [int(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5])]
                for c in data
            ]
        except Exception as e:
            self.logger.error(f"Error fetching OHLCV for {symbol} {timeframe}: {e}")
            return []

    def fetch_funding_rate(self, symbol: str, params: dict = None) -> Dict:
        """Fetch funding rate — CCXT style"""
        try:
            self._rate_limit()
            symbol_fmt = self.convert_symbol_format(symbol)
            data = self._public_request('/fapi/v1/premiumIndex', {'symbol': symbol_fmt})

            return {
                'symbol': symbol,
                'fundingRate': float(data.get('lastFundingRate', 0)),
                'fundingTimestamp': int(data.get('nextFundingTime', 0)),
                'info': data,
            }
        except Exception as e:
            self.logger.error(f"Error fetching funding rate for {symbol}: {e}")
            return {'fundingRate': 0}

    def fetch_trades(self, symbol: str, since: int = None, limit: int = 50, params: dict = None) -> List[Dict]:
        """Fetch recent trades — CCXT style"""
        try:
            self._rate_limit()
            symbol_fmt = self.convert_symbol_format(symbol)
            data = self._public_request('/fapi/v1/trades', {'symbol': symbol_fmt, 'limit': limit})

            if not isinstance(data, list):
                return []

            return [
                {
                    'id': str(t.get('id', '')),
                    'timestamp': t.get('time', 0),
                    'symbol': symbol,
                    'side': 'sell' if t.get('isBuyerMaker') else 'buy',
                    'price': float(t.get('price', 0)),
                    'amount': float(t.get('qty', 0)),
                    'info': t,
                }
                for t in data
            ]
        except Exception as e:
            self.logger.error(f"Error fetching trades for {symbol}: {e}")
            return []

    def fetch_my_trades(self, symbol: str = None, since: int = None, limit: int = 50, params: dict = None) -> List[Dict]:
        """Fetch user's own trades — CCXT style"""
        try:
            self._rate_limit()
            req_params = {'limit': limit}
            if symbol:
                req_params['symbol'] = self.convert_symbol_format(symbol)

            response = self._signed_request('GET', '/fapi/v1/userTrades', req_params)

            trades = []
            if isinstance(response, list):
                for t in response:
                    trades.append({
                        'id': str(t.get('id', '')),
                        'order': str(t.get('orderId', '')),
                        'timestamp': t.get('time', 0),
                        'symbol': t.get('symbol', ''),
                        'side': t.get('side', '').lower(),
                        'price': float(t.get('price', 0)),
                        'amount': float(t.get('qty', 0)),
                        'cost': float(t.get('quoteQty', 0)),
                        'fee': {
                            'cost': float(t.get('commission', 0)),
                            'currency': t.get('commissionAsset', 'USDT')
                        },
                        'info': t,
                    })
            return trades
        except Exception as e:
            self.logger.error(f"Error fetching my trades for {symbol}: {e}")
            return []

    def fetch_orders(self, symbol: str = None, since: int = None, limit: int = 50, params: dict = None) -> List[Dict]:
        """Fetch orders (open + recent closed) — CCXT style"""
        try:
            self._rate_limit()
            req_params = {'limit': limit}
            if symbol:
                req_params['symbol'] = self.convert_symbol_format(symbol)
            if params and 'orderLinkId' in params:
                req_params['origClientOrderId'] = params['orderLinkId']

            response = self._signed_request('GET', '/fapi/v1/allOrders', req_params)

            orders = []
            if isinstance(response, list):
                for o in response:
                    status_raw = o.get('status', '').upper()
                    status_map = {
                        'NEW': 'open', 'PARTIALLY_FILLED': 'open',
                        'FILLED': 'closed', 'CANCELED': 'canceled',
                        'REJECTED': 'rejected', 'EXPIRED': 'expired',
                    }
                    orders.append({
                        'id': str(o.get('orderId', '')),
                        'clientOrderId': o.get('clientOrderId', ''),
                        'symbol': o.get('symbol', ''),
                        'side': o.get('side', '').lower(),
                        'amount': float(o.get('origQty', 0)),
                        'filled': float(o.get('executedQty', 0)),
                        'price': float(o.get('price', 0)),
                        'average': float(o.get('avgPrice', 0)) if o.get('avgPrice') else None,
                        'status': status_map.get(status_raw, status_raw.lower()),
                        'fee': {},
                        'info': o,
                    })
            return orders
        except Exception as e:
            self.logger.error(f"Error fetching orders: {e}")
            return []

    def fetch_closed_orders(self, symbol: str = None, since: int = None, limit: int = 50, params: dict = None) -> List[Dict]:
        """Fetch closed (filled/cancelled) orders — CCXT style"""
        all_orders = self.fetch_orders(symbol, since, limit, params)
        return [o for o in all_orders if o.get('status') in ('closed', 'canceled', 'expired')]

    def create_order(self, symbol: str, type: str, side: str, amount: float, price: float = None, params: dict = None) -> Dict:
        """Create order — CCXT style"""
        params = params or {}
        position_side = params.get('positionSide')
        post_only = params.get('postOnly', True)
        order_link_id = params.get('orderLinkId')

        pos_idx = 0
        if position_side:
            pos_idx = 1 if position_side.lower() == 'long' else 2

        return self.place_order(
            symbol=symbol, side=side, amount=amount, price=price,
            order_type=type, post_only=post_only,
            position_idx=pos_idx, order_link_id=order_link_id,
        )

    def create_limit_buy_order(self, symbol: str, amount: float, price: float, params: dict = None) -> Dict:
        """Create limit buy order — CCXT style"""
        params = params or {}
        params['positionSide'] = params.get('positionSide', 'long')
        return self.create_order(symbol, 'limit', 'buy', amount, price, params)

    def create_limit_sell_order(self, symbol: str, amount: float, price: float, params: dict = None) -> Dict:
        """Create limit sell order — CCXT style"""
        params = params or {}
        params['positionSide'] = params.get('positionSide', 'short')
        return self.create_order(symbol, 'limit', 'sell', amount, price, params)

    def create_market_buy_order(self, symbol: str, amount: float, params: dict = None) -> Dict:
        """Create market buy order — CCXT style"""
        params = params or {}
        pos_side = params.get('positionSide', 'long')
        return self._place_market_order(symbol, 'buy', amount, pos_side)

    def create_market_sell_order(self, symbol: str, amount: float, params: dict = None) -> Dict:
        """Create market sell order — CCXT style"""
        params = params or {}
        pos_side = params.get('positionSide', 'short')
        return self._place_market_order(symbol, 'sell', amount, pos_side)

    # ========================================
    # CCXT-COMPATIBLE PROPERTIES
    # Strategies access .apiKey / .secret when creating spot exchange
    # ========================================

    @property
    def apiKey(self) -> str:
        """CCXT-compatible apiKey property."""
        return self.api_key or ''

    @property
    def secret(self) -> str:
        """CCXT-compatible secret property."""
        return self.api_secret or ''

    # ========================================
    # SINGLE ORDER QUERY
    # ========================================

    def fetch_order(self, order_id: str, symbol: str = None, params: dict = None) -> Dict:
        """Fetch a single order by ID — CCXT style.

        Used for order status checks, dual-side order tracking, etc.
        """
        try:
            self._rate_limit()
            req_params = {}

            if symbol:
                req_params['symbol'] = self.convert_symbol_format(symbol)

            # Support lookup by clientOrderId via params
            if params and params.get('orderLinkId'):
                req_params['origClientOrderId'] = params['orderLinkId']
            elif order_id:
                req_params['orderId'] = order_id

            response = self._signed_request('GET', '/fapi/v1/order', req_params)

            if response and 'orderId' in response:
                status_raw = response.get('status', '').upper()
                status_map = {
                    'NEW': 'open', 'PARTIALLY_FILLED': 'open',
                    'FILLED': 'closed', 'CANCELED': 'canceled',
                    'REJECTED': 'rejected', 'EXPIRED': 'expired',
                }
                return {
                    'id': str(response.get('orderId', '')),
                    'clientOrderId': response.get('clientOrderId', ''),
                    'symbol': response.get('symbol', ''),
                    'side': response.get('side', '').lower(),
                    'amount': float(response.get('origQty', 0)),
                    'filled': float(response.get('executedQty', 0)),
                    'price': float(response.get('price', 0)),
                    'average': float(response.get('avgPrice', 0)) if response.get('avgPrice') else None,
                    'status': status_map.get(status_raw, status_raw.lower()),
                    'type': response.get('type', '').lower(),
                    'reduceOnly': response.get('reduceOnly', False),
                    'info': response,
                }
            return {}
        except Exception as e:
            self.logger.error(f"Error fetching order {order_id}: {e}")
            return {}

    # ========================================
    # BALANCE (CCXT dict format)
    # ========================================

    def fetch_balance(self, params: dict = None) -> Dict:
        """Fetch balance — CCXT style dict format.

        Returns {'USDT': {'free': X, 'used': Y, 'total': Z}, 'total': {'USDT': Z}, ...}
        Used for equity calculations.
        """
        try:
            self._rate_limit()
            response = self._signed_request('GET', '/fapi/v1/account')

            result = {
                'info': response,
                'total': {},
                'free': {},
                'used': {},
            }

            if response:
                # Try totalWalletBalance first (single number)
                total_balance = float(response.get('totalWalletBalance', 0))
                available = float(response.get('availableBalance', 0))
                margin_used = float(response.get('totalInitialMargin', 0))

                if total_balance > 0:
                    result['USDT'] = {
                        'free': available,
                        'used': margin_used,
                        'total': total_balance,
                    }
                    result['total']['USDT'] = total_balance
                    result['free']['USDT'] = available
                    result['used']['USDT'] = margin_used
                elif 'assets' in response and isinstance(response['assets'], list):
                    for asset in response['assets']:
                        currency = asset.get('asset', '')
                        wallet = float(asset.get('walletBalance', 0))
                        avail = float(asset.get('availableBalance', 0))
                        used_margin = wallet - avail
                        result[currency] = {
                            'free': avail,
                            'used': max(0, used_margin),
                            'total': wallet,
                        }
                        result['total'][currency] = wallet
                        result['free'][currency] = avail
                        result['used'][currency] = max(0, used_margin)

            return result
        except Exception as e:
            self.logger.error(f"Error fetching balance: {e}")
            return {'USDT': {'free': 0, 'used': 0, 'total': 0}, 'total': {}, 'free': {}, 'used': {}}

    # ========================================
    # LATENCY STATS (stub for strategy compat)
    # ========================================

    def get_latency_stats(self) -> Dict:
        """Get latency percentiles — stub (REST only, no tracking yet)."""
        return {'p50': 0, 'p95': 0, 'p99': 0, 'count': 0}
