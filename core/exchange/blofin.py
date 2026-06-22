"""
BloFin Exchange Implementation
CCXT-based implementation for BloFin swap trading
"""

import ccxt
import time
import json
import hmac
import hashlib
import base64
import socket
import threading
from typing import List, Dict, Tuple, Optional
from .base import BaseExchange

try:
    import websocket
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False

# Owner's BloFin affiliate broker ID. Used as the default for every public
# user unless the config explicitly overrides 'broker_id'.
BLOFIN_BROKER_ID = "cc84bbde7d4b8a8c"


class BloFinWebSocketDataManager:
    """Real-time market data via BloFin WebSocket.

    Public: wss://openapi.blofin.com/ws/public
      - books.{INST_ID}       → 200-level orderbook (snapshot + delta)
      - trades.{INST_ID}      → trade feed
      - tickers.{INST_ID}     → last price, best bid/ask

    Private: wss://openapi.blofin.com/ws/private
      - positions             → real-time position updates
      - orders                → order fill notifications

    All data cached in memory. Strategy reads from cache (0ms) instead of REST (~250ms).
    Falls back to REST if WS disconnects.
    """

    PUBLIC_URL = "wss://openapi.blofin.com/ws/public"
    PUBLIC_URL_DEMO = "wss://demo-trading-openapi.blofin.com/ws/public"
    PRIVATE_URL = "wss://openapi.blofin.com/ws/private"
    PRIVATE_URL_DEMO = "wss://demo-trading-openapi.blofin.com/ws/private"

    def __init__(self, api_key: str, api_secret: str, password: str = '',
                 testnet: bool = False, logger=None, ccxt_exchange=None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.password = password  # BloFin requires passphrase
        self.testnet = testnet
        self.logger = logger
        self._ccxt_exchange = ccxt_exchange  # for contractSize lookups

        # Cached data (thread-safe via GIL for simple dict assignment)
        self._orderbooks: Dict[str, Dict] = {}
        self._tickers: Dict[str, Dict] = {}
        self._positions: Dict[str, Dict] = {}
        self._recent_trades: Dict[str, list] = {}

        self._position_seeded: set = set()
        self._contract_size_cache: Dict[str, float] = {}  # inst_id -> contract_size

        self._public_ws = None
        self._private_ws = None
        self._running = False
        self._connected_public = False
        self._connected_private = False
        self._subscribed_symbols: List[str] = []  # BloFin instId format: "BTC-USDT"

        self._default_position_side = {
            "qty": 0.0, "price": 0.0, "realised": 0, "cum_realised": 0,
            "upnl": 0, "upnl_pct": 0, "liq_price": 0, "entry_price": 0,
        }

    @staticmethod
    def _to_inst_id(symbol: str) -> str:
        """Convert BTCUSDT or BTC/USDT:USDT to BTC-USDT for BloFin WS."""
        if '-' in symbol and '/' not in symbol:
            return symbol  # Already BTC-USDT
        if '/' in symbol:
            # BTC/USDT:USDT -> BTC-USDT
            base = symbol.split('/')[0]
            return f"{base}-USDT"
        if symbol.endswith('USDT'):
            base = symbol[:-4]
            return f"{base}-USDT"
        return symbol

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
        return self._connected_public  # Public is essential

    def subscribe(self, symbol: str):
        """Subscribe to data streams for a symbol (any format accepted)."""
        inst_id = self._to_inst_id(symbol)
        if inst_id in self._subscribed_symbols:
            return

        self._subscribed_symbols.append(inst_id)

        # Initialize caches
        self._orderbooks[inst_id] = {'bids': [], 'asks': [], 'timestamp': 0}
        self._tickers[inst_id] = {'last': 0.0}
        self._recent_trades[inst_id] = []
        if inst_id not in self._positions:
            self._positions[inst_id] = {
                'long': dict(self._default_position_side),
                'short': dict(self._default_position_side),
            }

        if self._public_ws and self._connected_public:
            try:
                sub_msg = {
                    "op": "subscribe",
                    "args": [
                        {"channel": "books", "instId": inst_id},
                        {"channel": "trades", "instId": inst_id},
                        {"channel": "tickers", "instId": inst_id},
                    ]
                }
                self._public_ws.send(json.dumps(sub_msg))
                if self.logger:
                    self.logger.info(f"[WS-DATA] Subscribed public: {inst_id}")
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"[WS-DATA] Public subscribe failed: {e}")

    def _run_public(self):
        """Public WS connection loop with reconnection."""
        backoff = 1.0
        while self._running:
            try:
                url = self.PUBLIC_URL_DEMO if self.testnet else self.PUBLIC_URL
                ws = websocket.create_connection(url, timeout=30)
                self._public_ws = ws
                self._connected_public = True
                backoff = 1.0

                # Clear stale orderbook data from previous connection
                self._orderbooks.clear()

                if self.logger:
                    self.logger.info(f"[WS-DATA] Public connected: {url} (orderbook cache cleared)")

                # Subscribe to all current symbols
                for inst_id in self._subscribed_symbols:
                    sub_msg = {
                        "op": "subscribe",
                        "args": [
                            {"channel": "books", "instId": inst_id},
                            {"channel": "trades", "instId": inst_id},
                            {"channel": "tickers", "instId": inst_id},
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

                        # BloFin heartbeat: send "ping" string every 25s
                        now = time.time()
                        if now - last_ping > 25:
                            ws.send("ping")
                            last_ping = now

                        # BloFin pong is plain "pong" string
                        if msg == "pong":
                            continue

                        data = json.loads(msg)

                        # Route by channel in arg
                        arg = data.get('arg', {})
                        channel = arg.get('channel', '')
                        event = data.get('event', '')

                        if event:
                            # subscribe/error confirmation — skip
                            continue

                        if channel == 'books':
                            self._on_orderbook(data)
                        elif channel == 'trades':
                            self._on_public_trade(data)
                        elif channel == 'tickers':
                            self._on_ticker(data)

                    except websocket.WebSocketTimeoutException:
                        try:
                            ws.send("ping")
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
                url = self.PRIVATE_URL_DEMO if self.testnet else self.PRIVATE_URL
                ws = websocket.create_connection(url, timeout=30)

                # BloFin auth: matches CCXT — hmac→hex→base64(hex.encode())
                timestamp = str(int(time.time() * 1000))
                nonce = f"n_{timestamp}"
                auth_string = "/users/self/verifyGET" + timestamp + nonce
                hex_digest = hmac.new(
                    self.api_secret.encode('utf-8'),
                    auth_string.encode('utf-8'),
                    hashlib.sha256
                ).hexdigest()
                signature = base64.b64encode(hex_digest.encode('utf-8')).decode()

                login_msg = {
                    "op": "login",
                    "args": [{
                        "apiKey": self.api_key,
                        "passphrase": self.password,
                        "timestamp": timestamp,
                        "nonce": nonce,
                        "sign": signature,
                    }]
                }
                ws.send(json.dumps(login_msg))
                auth_resp = ws.recv()

                # Handle pong during auth
                if auth_resp == "pong":
                    auth_resp = ws.recv()

                auth_data = json.loads(auth_resp)
                event = auth_data.get('event', '')

                if event == 'error':
                    if self.logger:
                        self.logger.error(f"[WS-DATA] Private auth failed: {auth_data}")
                    ws.close()
                    time.sleep(backoff)
                    backoff = min(30.0, backoff * 2)
                    continue

                # Subscribe to positions and orders
                ws.send(json.dumps({
                    "op": "subscribe",
                    "args": [
                        {"channel": "positions"},
                        {"channel": "orders"},
                    ]
                }))

                self._private_ws = ws
                self._connected_private = True
                backoff = 1.0

                # Clear position seed cache — force REST reseed after reconnect
                # because BloFin only sends position deltas, not snapshots
                self._position_seeded.clear()

                if self.logger:
                    self.logger.info("[WS-DATA] Private connected and authenticated (position cache cleared for reseed)")

                # Receive loop
                last_ping = time.time()
                while self._running:
                    try:
                        msg = ws.recv()
                        if not msg:
                            continue

                        now = time.time()
                        if now - last_ping > 25:
                            ws.send("ping")
                            last_ping = now

                        if msg == "pong":
                            continue

                        data = json.loads(msg)
                        arg = data.get('arg', {})
                        channel = arg.get('channel', '')
                        event = data.get('event', '')

                        if event:
                            continue

                        if channel == 'positions':
                            self._on_position(data)
                        elif channel == 'orders':
                            self._on_execution(data)

                    except websocket.WebSocketTimeoutException:
                        try:
                            ws.send("ping")
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
        """Handle orderbook snapshot/delta updates."""
        arg = data.get('arg', {})
        inst_id = arg.get('instId', '')
        action = data.get('action', '')
        book_data = data.get('data', {})

        if not inst_id or not book_data:
            return

        # BloFin data is a dict with asks/bids arrays
        if isinstance(book_data, list):
            book_data = book_data[0] if book_data else {}

        if action == 'snapshot':
            bids = [[float(p), float(q)] for p, q in book_data.get('bids', [])]
            asks = [[float(p), float(q)] for p, q in book_data.get('asks', [])]
            self._orderbooks[inst_id] = {
                'bids': bids,
                'asks': asks,
                'timestamp': time.time(),
            }
        elif action == 'update':
            ob = self._orderbooks.get(inst_id)
            if not ob or not ob.get('bids'):
                return  # Wait for snapshot

            for price_str, qty_str in book_data.get('bids', []):
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
            # Trim to top 200 levels — BloFin sends unbounded deltas
            ob['bids'] = ob['bids'][:200]

            for price_str, qty_str in book_data.get('asks', []):
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
            # Trim to top 200 levels — BloFin sends unbounded deltas
            ob['asks'] = ob['asks'][:200]

            ob['timestamp'] = time.time()

    def _on_ticker(self, data: Dict):
        """Handle ticker updates."""
        arg = data.get('arg', {})
        inst_id = arg.get('instId', '')
        tick_list = data.get('data', [])

        if not inst_id or not tick_list:
            return

        tick_data = tick_list[0] if isinstance(tick_list, list) else tick_list

        existing = self._tickers.get(inst_id, {})
        # BloFin ticker fields
        if tick_data.get('askPrice'):
            existing['ask'] = float(tick_data['askPrice'])
        if tick_data.get('bidPrice'):
            existing['bid'] = float(tick_data['bidPrice'])
        if tick_data.get('askPrice') and tick_data.get('bidPrice'):
            existing['last'] = (float(tick_data['askPrice']) + float(tick_data['bidPrice'])) / 2
        # Some tickers include lastPrice directly
        if tick_data.get('last'):
            existing['last'] = float(tick_data['last'])
        if tick_data.get('lastPrice'):
            existing['last'] = float(tick_data['lastPrice'])
        existing['timestamp'] = time.time()
        self._tickers[inst_id] = existing

    def _on_public_trade(self, data: Dict):
        """Handle public trade updates."""
        arg = data.get('arg', {})
        inst_id = arg.get('instId', '')
        trades = data.get('data', [])

        if not inst_id or not trades:
            return

        trade_list = self._recent_trades.get(inst_id, [])
        for t in trades:
            ts_raw = t.get('ts', 0)
            if isinstance(ts_raw, str):
                ts_raw = int(ts_raw)
            trade_list.append({
                'price': float(t.get('price', 0)),
                'qty': float(t.get('size', 0)),
                'side': t.get('side', 'buy').lower(),
                'timestamp': ts_raw / 1000.0 if ts_raw > 1e12 else float(ts_raw),
            })

        if len(trade_list) > 500:
            trade_list = trade_list[-500:]
        self._recent_trades[inst_id] = trade_list

    def _on_position(self, data: Dict):
        """Handle private position updates."""
        positions = data.get('data', [])
        for pos in positions:
            inst_id = pos.get('instId', '')
            if not inst_id:
                continue

            pos_side = pos.get('positionSide', '').lower()
            if pos_side == 'long':
                side_key = 'long'
            elif pos_side == 'short':
                side_key = 'short'
            else:
                continue

            if inst_id not in self._positions:
                self._positions[inst_id] = {
                    'long': dict(self._default_position_side),
                    'short': dict(self._default_position_side),
                }

            # BloFin WS returns size in CONTRACTS — multiply by contractSize for tokens
            contracts = float(pos.get('positions', 0) or pos.get('size', 0) or 0)
            contract_size = self._get_contract_size(inst_id)
            # abs() because BloFin reports negative qty for shorts —
            # side is already determined by positionSide field above
            base_qty = abs(contracts * contract_size)

            if self.logger:
                self.logger.info(
                    f"[WS_POS] {inst_id} {side_key}: raw_contracts={contracts} "
                    f"contract_size={contract_size} base_qty={base_qty} "
                    f"(abs applied: {contracts < 0})")

            avg_price = float(pos.get('averagePrice', 0) or pos.get('avgPrice', 0) or 0)
            upnl_ratio = float(pos.get('unrealizedPnlRatio', 0) or 0)
            upnl_pct = round(upnl_ratio * 100, 4)

            self._positions[inst_id][side_key] = {
                'qty': base_qty,
                'price': avg_price,
                'realised': round(float(pos.get('realizedPnl', 0) or 0), 4),
                'cum_realised': round(float(pos.get('realizedPnl', 0) or 0), 4),
                'upnl': round(float(pos.get('unrealizedPnl', 0) or 0), 4),
                'upnl_pct': upnl_pct,
                'liq_price': float(pos.get('liquidationPrice', 0) or 0),
                'entry_price': avg_price,
            }

    def _on_execution(self, data: Dict):
        """Handle order/execution notifications — logged for awareness."""
        if self.logger:
            for exec_data in data.get('data', []):
                state = exec_data.get('state', '')
                if state in ('filled', 'partially_filled'):
                    self.logger.info(
                        f"[WS-EXEC] {exec_data.get('instId')} {exec_data.get('side')} "
                        f"qty={exec_data.get('filledSize', exec_data.get('size'))} "
                        f"@ {exec_data.get('avgPrice', exec_data.get('price'))} "
                        f"state={state}"
                    )

    def _get_contract_size(self, inst_id: str) -> float:
        """Get contract size for an instrument, cached after first lookup."""
        cached = self._contract_size_cache.get(inst_id)
        if cached is not None:
            return cached
        contract_size = 1.0
        if self._ccxt_exchange:
            try:
                ccxt_sym = inst_id.replace('-', '/') + ':USDT'
                mkt = self._ccxt_exchange.markets.get(ccxt_sym, {})
                contract_size = float(mkt.get('contractSize', 1) or 1)
            except Exception:
                pass
        self._contract_size_cache[inst_id] = contract_size
        if self.logger:
            self.logger.info(f"[WS-DATA] Cached contract_size for {inst_id}: {contract_size}")
        return contract_size

    # ── Cache getters with staleness check ──

    def get_orderbook(self, symbol: str) -> Optional[Dict]:
        """Get cached orderbook. Returns None if stale >5s."""
        inst_id = self._to_inst_id(symbol)
        ob = self._orderbooks.get(inst_id)
        if ob and ob.get('bids') and ob.get('asks'):
            if time.time() - ob.get('timestamp', 0) < 5.0:
                return ob
        return None

    def get_ticker(self, symbol: str) -> Optional[Dict]:
        """Get cached ticker. Returns None if stale >5s."""
        inst_id = self._to_inst_id(symbol)
        tick = self._tickers.get(inst_id)
        if tick and tick.get('last', 0) > 0:
            if time.time() - tick.get('timestamp', 0) < 5.0:
                return tick
        return None

    def get_positions(self, symbol: str) -> Optional[Dict]:
        """Get cached positions. Returns None if never received."""
        inst_id = self._to_inst_id(symbol)
        return self._positions.get(inst_id)

    def get_recent_trades(self, symbol: str) -> Optional[list]:
        """Get cached recent trades. Returns None if empty."""
        inst_id = self._to_inst_id(symbol)
        trades = self._recent_trades.get(inst_id)
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


class BloFinExchange(BaseExchange):
    """BloFin exchange implementation using CCXT"""

    id = 'blofin'  # Required for perp_market_maker._get_exchange_id()

    def __init__(self, config: Dict, logger=None):
        super().__init__(config, logger)
        self.rate_limit_delay = 0.1  # 100ms between requests

        # WebSocket data streams
        self.websocket_data_enabled = config.get('websocket_data', False)
        self._ws_data: Optional[BloFinWebSocketDataManager] = None

        if self.websocket_data_enabled and not WEBSOCKET_AVAILABLE:
            self.logger.warning("websocket_data enabled but websocket-client not installed — falling back to REST")
            self.websocket_data_enabled = False

    def connect(self) -> bool:
        """Connect to BloFin"""
        try:
            # Force IPv4 if configured (helps with IP whitelisting)
            if self.config.get('force_ipv4', True):
                self.logger.info("Forcing IPv4 connections for BloFin")
                original_getaddrinfo = socket.getaddrinfo
                def getaddrinfo_ipv4_only(*args):
                    return [info for info in original_getaddrinfo(*args) if info[0] == socket.AF_INET]
                socket.getaddrinfo = getaddrinfo_ipv4_only
            else:
                self.logger.info("Using default IPv4/IPv6 for BloFin")
            # Build options based on broker_id
            options = {'defaultType': 'swap'}  # BloFin only supports swap

            broker_id = self.config.get('broker_id') or BLOFIN_BROKER_ID
            options['brokerId'] = broker_id
            self.logger.info(f"Using broker ID: {broker_id}")

            self.exchange = ccxt.blofin({
                'apiKey': self.config['api_key'],
                'secret': self.config['api_secret'],
                'password': self.config.get('password', ''),  # BloFin requires password
                'sandbox': self.config.get('testnet', False),
                'enableRateLimit': True,
                'options': options
            })

            # Test connection
            self.exchange.load_markets()
            loaded_count = len(self.exchange.markets) if self.exchange.markets else 0
            usdt_markets = [s for s in (self.exchange.markets or {}) if s.endswith(':USDT')]
            self.logger.info(
                f"Successfully connected to BloFin — {loaded_count} markets loaded "
                f"({len(usdt_markets)} USDT perps)")
            if loaded_count < 50:
                self.logger.warning(
                    f"LOW MARKET COUNT: only {loaded_count} markets loaded — "
                    f"possible API issue, reloading...")
                self.exchange.load_markets(True)
                loaded_count = len(self.exchange.markets) if self.exchange.markets else 0
                self.logger.info(f"After reload: {loaded_count} markets")

            # Cache token-step / tick / min-notional so strategy quantization
            # (strategies/vortex/main.py::_quantize_for_exchange) finds the right
            # step for each symbol instead of falling through to a 1.0 default.
            # BloFin's CCXT precision is in CONTRACTS — convert to TOKENS via
            # precision.amount * contractSize. Bybit/HL/TxFlow do equivalent at
            # connect; BloFin previously skipped this, which over-rounded sub-1
            # token-step symbols (e.g. LINK true step 0.1 → forced to 1.0 → $9+
            # min lot when $5 floor would clear at 0.6 LINK).
            self._market_tick_sizes = {}
            self._market_qty_steps = {}
            self._market_min_order_values = {}
            for sym, market in (self.exchange.markets or {}).items():
                # Only USDT-margined perps — these are what the strategies trade
                # and what convert_symbol_format() round-trips.
                if not sym.endswith("/USDT:USDT"):
                    continue
                # Bybit-format key, e.g. "XRP/USDT:USDT" → "XRPUSDT" — matches
                # the symbol the strategy uses to look up.
                bybit_sym = sym.replace("/", "").replace(":USDT", "")
                contract_size = float(market.get("contractSize", 1) or 1)
                prec = market.get("precision", {}) or {}
                ccxt_step = prec.get("amount")
                ccxt_tick = prec.get("price")
                if isinstance(ccxt_step, (int, float)) and ccxt_step > 0:
                    token_step = float(ccxt_step) * contract_size
                    if token_step > 0:
                        self._market_qty_steps[bybit_sym] = token_step
                if isinstance(ccxt_tick, (int, float)) and ccxt_tick > 0:
                    self._market_tick_sizes[bybit_sym] = float(ccxt_tick)
                min_cost = (market.get("limits", {}) or {}).get("cost", {}).get("min")
                if isinstance(min_cost, (int, float)) and min_cost > 0:
                    self._market_min_order_values[bybit_sym] = float(min_cost)
            self.logger.info(
                f"BloFin precision cached: {len(self._market_qty_steps)} qty steps, "
                f"{len(self._market_tick_sizes)} tick sizes, "
                f"{len(self._market_min_order_values)} min-notionals"
            )
            # Sanity sample for the symbols most likely to surface the bug.
            for sample in ("LINKUSDT", "XRPUSDT", "ASTERUSDT"):
                step = self._market_qty_steps.get(sample)
                if step is not None:
                    self.logger.info(
                        f"  {sample}: token step={step} tick={self._market_tick_sizes.get(sample)}"
                    )

            # FIX: Set hedge mode IMMEDIATELY at connection time, not at first order
            # This prevents race condition where both sides of a quote fill before
            # hedge mode is active, causing them to cancel out in one-way mode
            self.setup_hedge_mode()
            self._hedge_mode_set = True

            # WebSocket data streams (real-time market data, 0ms reads)
            if self.websocket_data_enabled:
                self._ws_data = BloFinWebSocketDataManager(
                    api_key=self.config['api_key'],
                    api_secret=self.config['api_secret'],
                    password=self.config.get('password', ''),
                    testnet=self.config.get('testnet', False),
                    logger=self.logger,
                    ccxt_exchange=self.exchange,
                )
                if self._ws_data.connect():
                    self.logger.info("WebSocket DATA streams ENABLED (orderbook, trades, tickers, positions)")
                else:
                    # Don't null out — reconnection threads keep trying in background.
                    # Cached getters have staleness checks and REST fallback.
                    self.logger.warning("WebSocket data streams slow to connect — reconnecting in background, REST fallback active")
                self.rate_limit_delay = 0.01  # Reduce REST rate limit when WS configured

            return True

        except Exception as e:
            self.logger.error(f"Failed to connect to BloFin: {e}")
            return False
            
    def setup_hedge_mode(self) -> bool:
        """Setup hedge position mode if supported by BloFin"""
        try:
            # BloFin supports hedge mode via CCXT
            if hasattr(self.exchange, 'set_position_mode'):
                self.exchange.set_position_mode(hedged=True)
                self.logger.info("Set hedge position mode")
                return True
            else:
                self.logger.info("BloFin hedge mode not available via CCXT")
                return True  # Don't fail if not supported
        except Exception as e:
            if "not modified" in str(e).lower() or "already" in str(e).lower():
                self.logger.info("Hedge mode already enabled")
                return True
            self.logger.warning(f"Could not set hedge mode: {e}")
            return True  # Don't fail the connection
            
    def _rate_limit(self):
        """Apply rate limiting"""
        time.sleep(self.rate_limit_delay)
        
    def get_exchange_rules(self, symbol: str) -> dict:
        """Get exchange constraints — override base to convert symbol format.

        BloFin CCXT returns precision/limits in CONTRACTS, but the strategy
        works in TOKENS. We multiply by contractSize so _snap_and_validate
        produces token quantities, and place_order then divides back to contracts.

        Example: XRP contractSize=100
          CCXT: min_qty=0.01 contracts, qty_step=0.01 contracts
          Returned: min_qty=1.0 tokens, qty_step=1.0 tokens
        """
        converted = self.convert_symbol_format(symbol)
        try:
            market = self.exchange.market(converted)
            contract_size = float(market.get('contractSize', 1) or 1)

            # CCXT values are in contracts — convert to tokens
            ccxt_min_qty = market.get('limits', {}).get('amount', {}).get('min') or 1.0
            ccxt_qty_step = market.get('precision', {}).get('amount') or 1.0

            rules = {
                'min_order_value': market.get('limits', {}).get('cost', {}).get('min') or 5.0,
                'min_qty': ccxt_min_qty * contract_size,       # contracts -> tokens
                'qty_step': ccxt_qty_step * contract_size,     # contracts -> tokens
                'tick_size': market.get('precision', {}).get('price') or 0.0001,
                'contract_size': contract_size,
            }
            self.logger.info(
                f"get_exchange_rules({symbol}->{converted}): "
                f"min_val={rules['min_order_value']} "
                f"min_qty={rules['min_qty']}tok({ccxt_min_qty}ct) "
                f"qty_step={rules['qty_step']}tok({ccxt_qty_step}ct) "
                f"tick={rules['tick_size']} contract_size={contract_size}")
            return rules
        except Exception as e:
            self.logger.error(f"get_exchange_rules({symbol}->{converted}): {e}")
            return {'min_order_value': 5.0, 'min_qty': 1.0, 'qty_step': 1.0,
                    'tick_size': 0.0001, 'contract_size': 1.0}

    def convert_symbol_format(self, symbol: str) -> str:
        """Convert symbol format for BloFin futures"""
        if symbol.endswith('USDT') and '/' not in symbol:
            # Convert DOGEUSDT -> DOGE/USDT:USDT for futures
            base = symbol[:-4]  # Remove USDT suffix
            converted = f"{base}/USDT:USDT"
            self.logger.debug(f"BloFin symbol conversion: {symbol} -> {converted}")
            return converted
        return symbol
        
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
        """Get USDT available (free) margin.

        Mirrors BybitExchange.get_available_margin(): returns the FREE USDT,
        which shrinks as positions are opened or go underwater (cross margin
        respects leverage). Used by an external margin-guard.
        Distinct from get_balance() (total equity).
        """
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
            converted_symbol = self.convert_symbol_format(symbol)
            ticker = self.exchange.fetch_ticker(converted_symbol)
            return ticker['last']
        except Exception as e:
            err_str = str(e)
            if 'does not have market symbol' in err_str:
                # Markets may not have loaded properly — reload and retry once
                self.logger.warning(
                    f"Market not found for {converted_symbol} — reloading markets...")
                try:
                    self.exchange.load_markets(True)
                    loaded = len(self.exchange.markets) if self.exchange.markets else 0
                    has_sym = converted_symbol in (self.exchange.markets or {})
                    self.logger.info(
                        f"Markets reloaded: {loaded} total, "
                        f"{converted_symbol} {'FOUND' if has_sym else 'STILL MISSING'}")
                    if has_sym:
                        ticker = self.exchange.fetch_ticker(converted_symbol)
                        return ticker['last']
                except Exception as retry_e:
                    self.logger.error(f"Retry after reload also failed: {retry_e}")
            self.logger.error(f"Error fetching price for {converted_symbol} (input={symbol}): {e}")
            return 0.0

    def get_ticker(self, symbol: str) -> Dict:
        """Get ticker data for symbol"""
        try:
            self._rate_limit()
            converted_symbol = self.convert_symbol_format(symbol)
            return self.exchange.fetch_ticker(converted_symbol)
        except Exception as e:
            err_str = str(e)
            if 'does not have market symbol' in err_str:
                try:
                    self.exchange.load_markets(True)
                    if converted_symbol in (self.exchange.markets or {}):
                        return self.exchange.fetch_ticker(converted_symbol)
                except Exception:
                    pass
            self.logger.error(f"Error fetching ticker for {converted_symbol} (input={symbol}): {e}")
            return {'last': 0.0}

    def get_orderbook(self, symbol: str, limit: int = 50) -> Dict:
        """Get orderbook depth for symbol"""
        try:
            self._rate_limit()
            converted_symbol = self.convert_symbol_format(symbol)
            return self.exchange.fetch_order_book(converted_symbol, limit=limit)
        except Exception as e:
            err_str = str(e)
            if 'does not have market symbol' in err_str:
                try:
                    self.exchange.load_markets(True)
                    if converted_symbol in (self.exchange.markets or {}):
                        return self.exchange.fetch_order_book(converted_symbol, limit=limit)
                except Exception:
                    pass
            self.logger.error(f"Error fetching orderbook for {converted_symbol} (input={symbol}): {e}")
            return {'bids': [], 'asks': []}

    def get_positions(self, symbol) -> dict:
        """Get positions for symbol - matches Bybit format"""
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
            converted_symbol = self.convert_symbol_format(symbol)
            data = self.exchange.fetch_positions([converted_symbol])
            
            if len(data) >= 1:
                # Process each position and assign to correct side
                for pos in data:
                    # BloFin uses positionSide field
                    side = pos.get('side', '').lower()
                    
                    if side in ['long', 'buy']:
                        side_key = 'long'
                    elif side in ['short', 'sell']:
                        side_key = 'short'
                    else:
                        continue  # Skip unknown sides
                    
                    # BloFin returns contracts field, need to multiply by contract_size to get actual tokens
                    # For H/USDT:USDT, contract_size=10, so 395 contracts = 3950 H tokens
                    contracts = float(pos.get("contracts", 0) or 0)
                    contract_size = float(pos.get("contractSize", 1) or 1)

                    # CRITICAL FIX: Multiply by contract_size to get actual token quantity
                    # CCXT returns contracts, but we need base token amount
                    # abs() because BloFin reports negative qty for shorts —
                    # side is already determined by positionSide field above
                    base_qty = abs(contracts * contract_size)

                    self.logger.info(
                        f"[REST_POS] {symbol} {side_key}: raw_contracts={contracts} "
                        f"contract_size={contract_size} base_qty={base_qty} "
                        f"(abs applied: {contracts < 0})")
                    
                    # Map BloFin position data to expected format
                    values[side_key]["qty"] = base_qty
                    values[side_key]["price"] = float(pos.get("entryPrice", 0) or 0)
                    values[side_key]["realised"] = round(float(pos.get("realizedPnl", 0) or 0), 4)
                    values[side_key]["cum_realised"] = round(float(pos.get("realizedPnl", 0) or 0), 4)
                    values[side_key]["upnl"] = round(float(pos.get("unrealizedPnl", 0) or 0), 4)
                    values[side_key]["upnl_pct"] = round(float(pos.get("percentage", 0) or 0), 4)
                    values[side_key]["liq_price"] = float(pos.get("liquidationPrice", 0) or 0)
                    values[side_key]["entry_price"] = float(pos.get("entryPrice", 0) or 0)
                        
            self.logger.info(f"Positions for {symbol}: Long={values['long']['qty']}, Short={values['short']['qty']}")
        except Exception as e:
            self.logger.error(f"Error getting positions for {symbol}: {e}")
        return values
            
    def get_open_orders(self, symbol: str = None) -> List[Dict]:
        """Get open orders"""
        try:
            self._rate_limit()
            if symbol:
                converted_symbol = self.convert_symbol_format(symbol)
                orders = self.exchange.fetch_open_orders(converted_symbol)
            else:
                orders = self.exchange.fetch_open_orders()
                
            formatted_orders = []
            markets = self.exchange.load_markets()
            for order in orders:
                # CCXT returns amount in contracts — convert to tokens
                raw_amount = float(order['amount'] or 0)
                sym = order['symbol']
                contract_size = float(markets.get(sym, {}).get('contractSize', 1) or 1)
                token_amount = raw_amount * contract_size

                formatted_orders.append({
                    'id': order['id'],
                    'symbol': order['symbol'],
                    'side': order['side'],
                    'amount': token_amount,
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
        position_side: str = None,
        post_only: bool = False,
        position_idx: int = None,
        leverage=None,
    ) -> Dict:
        """Place an order.

        leverage: optional per-order leverage override. Default None preserves
        the historical 20x default (other callers are unaffected).
        A caller may pass its own lower leverage so its risk envelope is honored
        instead of silently running at 20x.
        """
        try:            
            # Convert symbol format
            converted_symbol = self.convert_symbol_format(symbol)
            
            # Get precision and limits
            precision_amount, precision_price, min_amount = self.get_precision_and_limits(converted_symbol)
            if precision_amount is None:
                self.logger.error(f"Could not get precision for {symbol}")
                return {}
            
            # Get contract size for BloFin if applicable
            markets = self.exchange.load_markets()
            market = markets.get(converted_symbol, {})
            contract_size = market.get('contractSize', 1.0)
            
            # CRITICAL FIX: For BloFin, CCXT's amount parameter is in CONTRACTS, not tokens!
            # Bot calculates in tokens, but we must convert to contracts for the order
            original_amount_tokens = amount
            amount_in_contracts = amount / contract_size

            self.logger.info(f"BloFin order conversion: {original_amount_tokens:.4f} tokens / {contract_size} = {amount_in_contracts:.4f} contracts")

            # Use contracts for the order
            amount = amount_in_contracts

            # Format amount and price
            if precision_amount is not None:
                if isinstance(precision_amount, float):
                    amount_str = f"{amount:.10f}"
                    amount = float(amount_str.rstrip('0').rstrip('.'))
                else:
                    amount = round(amount, precision_amount)
            
            if price is not None and precision_price is not None:
                if isinstance(precision_price, float):
                    price_str = f"{price:.10f}"
                    price = float(price_str.rstrip('0').rstrip('.'))
                else:
                    price = round(price, precision_price)
                
            # Check minimum quantity and apply rounding if enabled
            if min_amount and amount < min_amount:
                # Check if round_min_qty_blofin is enabled in config
                round_min_qty = self.config.get('round_min_qty_blofin', False)

                if round_min_qty:
                    self.logger.warning(f"Order quantity {amount} below minimum {min_amount}, rounding up to {min_amount}")
                    amount = min_amount
                else:
                    self.logger.error(f"Order quantity {amount} below minimum {min_amount}")
                    return {}
                
            self._rate_limit()
            
            # Setup hedge mode once if not already done
            if not hasattr(self, '_hedge_mode_set'):
                self.setup_hedge_mode()
                self._hedge_mode_set = True
            
            # BloFin-specific parameters
            params = {}
            if reduce_only:
                params['reduceOnly'] = True  # CCXT converts to string internally

            # Set post-only for maker orders (avoid taker fees)
            if post_only:
                params['postOnly'] = True
                self.logger.info(f"POST-ONLY enabled for {side} {amount} @ {price}")

            # Map position_idx (Bybit-style) to position_side (BloFin-style)
            if position_idx is not None and position_side is None:
                if position_idx == 1:
                    position_side = 'long'
                elif position_idx == 2:
                    position_side = 'short'

            # Set position side for hedge mode
            # For reduce-only orders, positionSide should match the position being closed
            if reduce_only and position_side:
                params['positionSide'] = position_side  # Use explicit position side
            else:
                # For regular orders, positionSide matches order side
                params['positionSide'] = 'long' if side == 'buy' else 'short'

            # BloFin requires a leverage parameter for swap orders. Default 20x
            # (HTX-style) is preserved for existing callers; a caller may pass an
            # explicit, lower leverage (a caller may) — additive override.
            params['leverage'] = str(leverage) if leverage is not None else '20'

            # For BloFin, use 'post_only' as order type directly (not just param)
            actual_order_type = 'post_only' if post_only else order_type

            # DEBUG: Log params being sent
            self.logger.info(f"ORDER PARAMS: type={actual_order_type}, reduceOnly={params.get('reduceOnly')}, positionSide={params.get('positionSide')}")

            order = self.exchange.create_order(
                symbol=converted_symbol,
                type=actual_order_type,
                side=side,
                amount=amount,
                price=price,
                params=params
            )

            self.logger.info(f"Placed {side} order: {symbol} {amount} @ {price}")
            return {
                'id': order['id'],
                'symbol': order['symbol'],
                'side': order['side'],
                'amount': order['amount'],
                'price': order['price'],
                'status': order['status']
            }
            
        except Exception as e:
            self.logger.error(f"Error placing order: {e}")
            return {}
            
    def place_orders_batch(
        self,
        orders: List[Dict],
        symbol: str,
        position_idx: int = 0,
        skip_cancel_ids: List[str] = None,
    ) -> List[Dict]:
        """
        Atomic cancel-and-place via CCXT batch API.

        1. Cancel all open non-protected orders for this symbol
        2. Place new orders in one batch REST call (up to 20)

        Returns list of result dicts with {id, symbol, side, status}.
        """
        if not orders:
            return []

        converted_symbol = self.convert_symbol_format(symbol)
        t_start = time.perf_counter()

        # --- Step 1: Cancel existing MM orders (skip protected IDs like TP / recovery) ---
        try:
            open_orders = self.get_open_orders(symbol)
            cancel_ids = []
            protected = set(skip_cancel_ids or [])
            for o in open_orders:
                if o['id'] not in protected and not o.get('reduce_only', False):
                    cancel_ids.append(o['id'])

            if cancel_ids:
                cancel_params = [
                    {'id': oid, 'symbol': converted_symbol}
                    for oid in cancel_ids
                ]
                # Batch cancel up to 20 at a time
                for i in range(0, len(cancel_params), 20):
                    chunk = cancel_params[i:i+20]
                    try:
                        self.exchange.cancel_orders(
                            [c['id'] for c in chunk],
                            converted_symbol
                        )
                    except Exception as e:
                        self.logger.warning(f"[BATCH-CANCEL] partial fail: {e}")
                self.logger.info(f"[BATCH-CANCEL] cancelled {len(cancel_ids)} orders for {symbol}")
        except Exception as e:
            self.logger.warning(f"[BATCH-CANCEL] cancel phase failed: {e}")

        # --- Step 2: Build CCXT order list ---
        markets = self.exchange.load_markets()
        market = markets.get(converted_symbol, {})
        contract_size = float(market.get('contractSize', 1) or 1)
        precision_amount = market.get('precision', {}).get('amount')
        precision_price = market.get('precision', {}).get('price')

        ccxt_orders = []
        for order in orders:
            side = order.get('side', 'buy')
            amount = order.get('amount', 0)
            price = order.get('price')
            reduce_only = order.get('reduce_only', False)
            post_only = order.get('post_only', True)

            # Convert tokens to contracts
            amount_contracts = amount / contract_size

            # Apply precision
            if precision_amount is not None:
                if isinstance(precision_amount, float):
                    amount_str = f"{amount_contracts:.10f}"
                    amount_contracts = float(amount_str.rstrip('0').rstrip('.'))
                else:
                    amount_contracts = round(amount_contracts, precision_amount)

            if price is not None and precision_price is not None:
                if isinstance(precision_price, float):
                    price_str = f"{price:.10f}"
                    price = float(price_str.rstrip('0').rstrip('.'))
                else:
                    price = round(price, precision_price)

            params = {
                'positionSide': 'long' if side == 'buy' else 'short',
            }
            if reduce_only:
                params['reduceOnly'] = True
            if post_only:
                params['postOnly'] = True

            ccxt_orders.append({
                'symbol': converted_symbol,
                'type': 'limit',
                'side': side,
                'amount': amount_contracts,
                'price': price,
                'params': params,
            })

        # --- Step 3: Place batch via CCXT ---
        results = []
        try:
            self._rate_limit()
            # CCXT create_orders handles BloFin's batch-orders endpoint
            responses = self.exchange.create_orders(ccxt_orders)

            for i, resp in enumerate(responses):
                if resp and resp.get('id'):
                    results.append({
                        'id': resp['id'],
                        'symbol': resp.get('symbol', converted_symbol),
                        'side': resp.get('side', orders[i].get('side', '')),
                        'amount': resp.get('amount', 0),
                        'price': resp.get('price', 0),
                        'status': resp.get('status', 'open'),
                    })
                else:
                    results.append({
                        'symbol': converted_symbol,
                        'side': orders[i].get('side', ''),
                        'status': 'failed',
                    })

            t_ms = (time.perf_counter() - t_start) * 1000
            ok = sum(1 for r in results if r.get('id'))
            self.logger.info(
                f"[BATCH-PLACE] {ok}/{len(orders)} placed in {t_ms:.0f}ms for {symbol}")

        except Exception as e:
            t_ms = (time.perf_counter() - t_start) * 1000
            self.logger.error(f"[BATCH-PLACE] failed: {e} ({t_ms:.0f}ms)")

        return results

    def cancel_orders_batch(
        self,
        orders: List[Dict],
        symbol: str,
    ) -> List[bool]:
        """
        Cancel multiple orders via CCXT batch API.

        Args:
            orders: List of dicts with 'order_id' or 'id' key
            symbol: Trading symbol

        Returns:
            List of bools indicating success/failure for each cancel
        """
        if not orders:
            return []

        converted_symbol = self.convert_symbol_format(symbol)
        t_start = time.perf_counter()

        order_ids = []
        for o in orders:
            oid = o.get('order_id') or o.get('id') or ''
            if oid:
                order_ids.append(oid)

        if not order_ids:
            return []

        results = [False] * len(order_ids)
        try:
            self._rate_limit()
            # Process in chunks of 20 (BloFin limit)
            for chunk_start in range(0, len(order_ids), 20):
                chunk = order_ids[chunk_start:chunk_start + 20]
                try:
                    self.exchange.cancel_orders(chunk, converted_symbol)
                    # If no exception, mark all in chunk as success
                    for j in range(len(chunk)):
                        results[chunk_start + j] = True
                except Exception as e:
                    self.logger.warning(f"[BATCH-CANCEL] chunk failed: {e}")
                    # Try individual cancels for this chunk
                    for j, oid in enumerate(chunk):
                        try:
                            self.exchange.cancel_order(oid, converted_symbol)
                            results[chunk_start + j] = True
                        except Exception:
                            pass

            t_ms = (time.perf_counter() - t_start) * 1000
            ok = sum(results)
            self.logger.info(
                f"[BATCH-CANCEL] {ok}/{len(order_ids)} cancelled in {t_ms:.0f}ms for {symbol}")

        except Exception as e:
            t_ms = (time.perf_counter() - t_start) * 1000
            self.logger.error(f"[BATCH-CANCEL] failed: {e} ({t_ms:.0f}ms)")

        return results

    def cancel_order(self, order_id: str, symbol: str) -> bool:
        """Cancel specific order"""
        try:
            self._rate_limit()
            converted_symbol = self.convert_symbol_format(symbol)
            self.exchange.cancel_order(order_id, converted_symbol)
            self.logger.info(f"Cancelled order {order_id} for {symbol}")
            return True
        except Exception as e:
            self.logger.error(f"Error cancelling order {order_id}: {e}")
            return False
            
    def cancel_all_orders(self, symbol: str = None) -> bool:
        """Cancel all orders for symbol or all symbols"""
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
                # Cancel ALL orders across ALL symbols
                self.logger.info("Cancelling ALL orders across entire account...")
                self._rate_limit()
                
                all_orders = self.get_open_orders()  # No symbol = all orders
                self.logger.info(f"Found {len(all_orders)} total orders across all symbols")
                
                cancelled_count = 0
                symbols_with_orders = set()
                
                for order in all_orders:
                    try:
                        order_symbol = order.get('symbol', 'UNKNOWN')
                        symbols_with_orders.add(order_symbol)
                        
                        if self.cancel_order(order['id'], order_symbol):
                            cancelled_count += 1
                            self.logger.debug(f"[{order_symbol}] Cancelled order {order['id']}")
                        else:
                            self.logger.warning(f"[{order_symbol}] Failed to cancel order {order['id']}")
                            
                    except Exception as e:
                        self.logger.warning(f"Failed to cancel order {order.get('id', 'UNKNOWN')}: {e}")
                
                self.logger.info(f"✅ Cancelled {cancelled_count}/{len(all_orders)} orders across symbols: {sorted(symbols_with_orders)}")
                return cancelled_count == len(all_orders)
                
        except Exception as e:
            self.logger.error(f"Error cancelling all orders: {e}")
            return False
            
    def place_take_profit_order(self, symbol: str, side: str, amount: float, price: float, position_side: str = None) -> Dict:
        """Place a take profit (reduce-only) order"""
        # For BloFin hedge mode, we need to specify which position side to close
        if position_side is None:
            # Infer position side from order side: sell order closes long position
            position_side = 'long' if side == 'sell' else 'short'

        # Use the main place_order method with position_side and post_only
        return self.place_order(
            symbol=symbol,
            side=side,
            amount=amount,
            price=price,
            order_type="limit",
            reduce_only=True,
            position_side=position_side,
            post_only=True  # Use maker orders for TP
        )

    def create_limit_buy_order(self, symbol: str, amount: float, price: float, params: dict = None) -> Dict:
        """CCXT-compatible create_limit_buy_order - always uses post_only for maker fees"""
        params = params or {}
        position_side = params.get('positionSide', 'long')
        reduce_only = params.get('reduceOnly', False)
        if isinstance(reduce_only, str):
            reduce_only = reduce_only.lower() == 'true'

        return self.place_order(
            symbol=symbol,
            side='buy',
            amount=amount,
            price=price,
            order_type='limit',
            reduce_only=reduce_only,
            position_side=position_side,
            post_only=True  # Always maker for MM
        )

    def create_limit_sell_order(self, symbol: str, amount: float, price: float, params: dict = None) -> Dict:
        """CCXT-compatible create_limit_sell_order - always uses post_only for maker fees"""
        params = params or {}
        position_side = params.get('positionSide', 'short')
        reduce_only = params.get('reduceOnly', False)
        if isinstance(reduce_only, str):
            reduce_only = reduce_only.lower() == 'true'

        return self.place_order(
            symbol=symbol,
            side='sell',
            amount=amount,
            price=price,
            order_type='limit',
            reduce_only=reduce_only,
            position_side=position_side,
            post_only=True  # Always maker for MM
        )

    def create_market_buy_order(self, symbol: str, amount: float, params: dict = None) -> Dict:
        """CCXT-compatible create_market_buy_order for emergency exits"""
        params = params or {}
        position_side = params.get('positionSide', 'long')
        reduce_only = params.get('reduceOnly', False)
        if isinstance(reduce_only, str):
            reduce_only = reduce_only.lower() == 'true'

        return self.place_order(
            symbol=symbol,
            side='buy',
            amount=amount,
            price=None,
            order_type='market',
            reduce_only=reduce_only,
            position_side=position_side,
            post_only=False
        )

    def create_market_sell_order(self, symbol: str, amount: float, params: dict = None) -> Dict:
        """CCXT-compatible create_market_sell_order for emergency exits"""
        params = params or {}
        position_side = params.get('positionSide', 'short')
        reduce_only = params.get('reduceOnly', False)
        if isinstance(reduce_only, str):
            reduce_only = reduce_only.lower() == 'true'

        return self.place_order(
            symbol=symbol,
            side='sell',
            amount=amount,
            price=None,
            order_type='market',
            reduce_only=reduce_only,
            position_side=position_side,
            post_only=False
        )

    def create_order(self, symbol: str, type: str, side: str, amount: float, price: float = None, params: dict = None) -> Dict:
        """
        Generic create_order method that routes to specific order methods.
        This provides compatibility with code expecting ccxt-style create_order interface.
        """
        params = params or {}

        # Extract BloFin-specific params
        position_idx = params.get('positionIdx')
        reduce_only = params.get('reduceOnly', False)
        time_in_force = params.get('timeInForce', '')

        # Build params for the specific method
        order_params = {}

        # Handle position side for hedge mode
        if position_idx == 1:
            order_params['positionSide'] = 'long'
        elif position_idx == 2:
            order_params['positionSide'] = 'short'
        elif reduce_only:
            # For reduce-only, position side is opposite of order side
            order_params['positionSide'] = 'short' if side == 'buy' else 'long'

        if reduce_only:
            order_params['reduceOnly'] = True

        # Route to appropriate method
        if type == 'market':
            if side == 'buy':
                return self.create_market_buy_order(symbol, amount, order_params)
            else:
                return self.create_market_sell_order(symbol, amount, order_params)
        else:
            # limit or post_only
            post_only = (type == 'post_only' or time_in_force == 'PostOnly')
            if post_only:
                order_params['post_only'] = True

            if side == 'buy':
                return self.create_limit_buy_order(symbol, amount, price, order_params)
            else:
                return self.create_limit_sell_order(symbol, amount, price, order_params)

    def get_precision_and_limits(self, symbol: str) -> tuple:
        """Get precision and limits for a symbol"""
        try:
            markets = self.exchange.load_markets()
            if symbol in markets:
                market = markets[symbol]
                precision_amount = market['precision']['amount']
                precision_price = market['precision']['price']
                min_amount = market['limits']['amount']['min']
                return precision_amount, precision_price, min_amount
            else:
                self.logger.error(f"Symbol {symbol} not found in markets")
                return None, None, None
        except Exception as e:
            self.logger.error(f"Error getting precision for {symbol}: {e}")
            return None, None, None

    # ============================================================
    # CCXT-Compatible Proxy Methods for Perp Market Maker
    # These delegate to the internal CCXT exchange instance
    # ============================================================

    def load_markets(self, reload: bool = False) -> Dict:
        """CCXT-compatible load_markets proxy"""
        return self.exchange.load_markets(reload)

    def set_leverage(self, symbol_or_leverage, leverage_or_symbol=None, params: dict = None) -> Dict:
        """Set leverage — accepts both (symbol, leverage) and (leverage, symbol) arg order."""
        if isinstance(symbol_or_leverage, str):
            symbol = symbol_or_leverage
            leverage = leverage_or_symbol
        else:
            leverage = symbol_or_leverage
            symbol = leverage_or_symbol
        converted_symbol = self.convert_symbol_format(symbol)
        self._rate_limit()
        try:
            result = self.exchange.set_leverage(leverage, converted_symbol, params or {})
            self.logger.info(f"Set leverage to {leverage}x for {symbol}")
            return result
        except Exception as e:
            if "not modified" in str(e).lower() or "already" in str(e).lower():
                self.logger.info(f"Leverage already set to {leverage}x for {symbol}")
                return {}
            raise

    def fetch_positions(self, symbols: List[str] = None, params: dict = None) -> List[Dict]:
        """CCXT-compatible fetch_positions proxy"""
        self._rate_limit()
        if symbols:
            converted_symbols = [self.convert_symbol_format(s) for s in symbols]
            return self.exchange.fetch_positions(converted_symbols, params or {})
        return self.exchange.fetch_positions(params=params or {})

    def fetch_funding_rate(self, symbol: str, params: dict = None) -> Dict:
        """CCXT-compatible fetch_funding_rate proxy"""
        converted_symbol = self.convert_symbol_format(symbol)
        self._rate_limit()
        return self.exchange.fetch_funding_rate(converted_symbol, params or {})

    def fetch_open_orders(self, symbol: str = None, since: int = None, limit: int = None, params: dict = None) -> List[Dict]:
        """CCXT-compatible fetch_open_orders proxy"""
        self._rate_limit()
        if symbol:
            converted_symbol = self.convert_symbol_format(symbol)
            return self.exchange.fetch_open_orders(converted_symbol, since, limit, params or {})
        return self.exchange.fetch_open_orders(None, since, limit, params or {})

    def fetch_order_book(self, symbol: str, limit: int = None, params: dict = None) -> Dict:
        """CCXT-compatible fetch_order_book proxy"""
        converted_symbol = self.convert_symbol_format(symbol)
        self._rate_limit()
        return self.exchange.fetch_order_book(converted_symbol, limit, params or {})

    def fetch_ticker(self, symbol: str, params: dict = None) -> Dict:
        """CCXT-compatible fetch_ticker proxy"""
        converted_symbol = self.convert_symbol_format(symbol)
        self._rate_limit()
        return self.exchange.fetch_ticker(converted_symbol, params or {})

    def fetch_my_trades(self, symbol: str = None, since: int = None, limit: int = None, params: dict = None) -> List[Dict]:
        """CCXT-compatible fetch_my_trades proxy"""
        self._rate_limit()
        if symbol:
            converted_symbol = self.convert_symbol_format(symbol)
            return self.exchange.fetch_my_trades(converted_symbol, since, limit, params or {})
        return self.exchange.fetch_my_trades(None, since, limit, params or {})

    def fetch_balance(self, params: dict = None) -> Dict:
        """CCXT-compatible fetch_balance proxy"""
        self._rate_limit()
        return self.exchange.fetch_balance(params or {})

    def get_recent_trades(self, symbol: str, limit: int = 200) -> List[Dict]:
        """Get recent public trades. Returns list of {price, qty, side, timestamp}."""
        try:
            self._rate_limit()
            converted_symbol = self.convert_symbol_format(symbol)
            trades = self.exchange.fetch_trades(converted_symbol, limit=limit)
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

    def get_klines(self, symbol: str, interval: str = '1m', limit: int = 20) -> List[Dict]:
        """Get OHLCV klines via ccxt. Returns list of {timestamp, open, high, low, close}."""
        try:
            self._rate_limit()
            converted_symbol = self.convert_symbol_format(symbol)
            ohlcv = self.exchange.fetch_ohlcv(converted_symbol, timeframe=interval, limit=limit)
            return [
                {'timestamp': c[0], 'open': c[1], 'high': c[2], 'low': c[3], 'close': c[4]}
                for c in ohlcv
            ]
        except Exception as e:
            self.logger.warning(f"Error fetching klines for {symbol}: {e}")
            return []

    def fetch_ohlcv(self, symbol: str, timeframe: str = '1m', since: int = None, limit: int = None, params: dict = None) -> List:
        """CCXT-compatible fetch_ohlcv proxy for candle data"""
        self._rate_limit()
        return self.exchange.fetch_ohlcv(symbol, timeframe, since, limit, params or {})

    # ============================================================
    # WebSocket Cached Getters (0ms reads with REST fallback)
    # ============================================================

    def subscribe_data(self, symbol: str):
        """Subscribe a symbol on the WS data manager.

        Named subscribe_data to match the interface expected by strategies
        (the strategy checks hasattr(exchange, 'subscribe_data')).
        """
        if self._ws_data:
            self._ws_data.subscribe(symbol)

    def get_current_price_cached(self, symbol: str) -> float:
        """Get price from WS cache (0ms) or REST fallback."""
        if self._ws_data:
            ticker = self._ws_data.get_ticker(symbol)
            if ticker and ticker.get('last', 0) > 0:
                return ticker['last']
        return self.get_current_price(symbol)

    def get_orderbook_cached(self, symbol: str, limit: int = 50) -> Dict:
        """Get orderbook from WS cache (0ms) or REST fallback."""
        if self._ws_data:
            ob = self._ws_data.get_orderbook(symbol)
            if ob and ob.get('bids') and ob.get('asks'):
                return ob
        return self.get_orderbook(symbol, limit)

    def get_positions_cached(self, symbol: str) -> Dict:
        """Get positions from WS cache (0ms) or REST fallback.

        WS position updates only arrive on CHANGE, so we periodically
        verify against REST to catch positions opened externally or
        missed WS updates.
        """
        if self._ws_data:
            import time
            inst_id = BloFinWebSocketDataManager._to_inst_id(symbol)
            pos = self._ws_data.get_positions(symbol)
            if pos:
                has_qty = (
                    float(pos.get('long', {}).get('qty', 0)) != 0
                    or float(pos.get('short', {}).get('qty', 0)) != 0
                )
                # Periodically verify WS cache against REST to catch stale data
                # - WS shows zero: verify every 30s (position opened externally?)
                # - WS shows non-zero: verify every 60s (position closed but WS missed?)
                verify_interval = 60 if has_qty else 30
                now = time.time()
                last_check = getattr(self, '_position_rest_check_ts', {}).get(inst_id, 0)
                if now - last_check > verify_interval:
                    if not hasattr(self, '_position_rest_check_ts'):
                        self._position_rest_check_ts = {}
                    self._position_rest_check_ts[inst_id] = now
                    rest_pos = self.get_positions(symbol)
                    if rest_pos:
                        rest_has_qty = (
                            float(rest_pos.get('long', {}).get('qty', 0)) != 0
                            or float(rest_pos.get('short', {}).get('qty', 0)) != 0
                        )
                        if rest_has_qty != has_qty:
                            # WS and REST disagree — REST wins
                            self._ws_data._positions[inst_id] = rest_pos
                            self.logger.warning(
                                f"[{symbol}] WS/REST position mismatch — "
                                f"WS has_qty={has_qty} REST has_qty={rest_has_qty} — reseeded from REST")
                            return rest_pos
                        if rest_has_qty and has_qty:
                            # Both have qty but check if qty changed significantly
                            for side in ('long', 'short'):
                                ws_qty = float(pos.get(side, {}).get('qty', 0))
                                rest_qty = float(rest_pos.get(side, {}).get('qty', 0))
                                if ws_qty != rest_qty:
                                    self._ws_data._positions[inst_id] = rest_pos
                                    self.logger.warning(
                                        f"[{symbol}] WS/REST qty mismatch {side}: "
                                        f"WS={ws_qty} REST={rest_qty} — reseeded from REST")
                                    return rest_pos
                return pos
        return self.get_positions(symbol)

    def get_recent_trades_cached(self, symbol: str, limit: int = 200) -> List[Dict]:
        """Get recent trades from WS cache (0ms) or REST fallback."""
        if self._ws_data:
            trades = self._ws_data.get_recent_trades(symbol)
            if trades:
                return trades[-limit:]
        return self.get_recent_trades(symbol, limit)

    def fetch_order_book_cached(self, symbol: str, limit: int = 50, params=None) -> Dict:
        """CCXT-compatible fetch_order_book from WS cache."""
        if self._ws_data:
            ob = self._ws_data.get_orderbook(symbol)
            if ob and ob.get('bids') and ob.get('asks'):
                return ob
        return self.fetch_order_book(symbol, limit, params)

    def fetch_trades_cached(self, symbol: str, since=None, limit: int = 50, params=None) -> list:
        """CCXT-compatible fetch_trades from WS cache."""
        if self._ws_data:
            trades = self._ws_data.get_recent_trades(symbol)
            if trades:
                return trades[-limit:]
        self._rate_limit()
        converted_symbol = self.convert_symbol_format(symbol)
        return self.exchange.fetch_trades(converted_symbol, since, limit, params or {})