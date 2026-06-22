"""
Vortex DCA Strategy - Clean Implementation
Main strategy class that orchestrates the Vortex DCA algorithm
"""

import copy
import time
import threading
from typing import Any, Dict, List, Optional, Tuple
from .calculator import VortexDCACalculator
from .virtual_chunking_calculator import VirtualChunkingCalculator
from .candle_store import CandleStore
from .regime_detector import RegimeDetector
from .wave_queue import resolve_auto_max_waves, validate_wave_queue_config


class VortexDCAStrategy:
    """Main Vortex DCA strategy implementation"""
    
    def __init__(self, exchange, config: Dict, logger):
        self.exchange = exchange
        self.config = config
        self.logger = logger

        # Initialize candle store for historical clustering
        self.candle_store = CandleStore(exchange, logger)
        self.logger.info("Candle store initialized for historical clustering support")

        # Strategy state
        self.calculators = {}  # symbol -> VortexDCACalculator
        self.last_refresh_prices = {}  # symbol -> price
        self.running = False
        self.threads = {}  # symbol -> thread
        self.locks = {}  # symbol -> lock

        # Virtual chunking calculator for position recovery
        self.virtual_chunking = VirtualChunkingCalculator(config.get('vortex_dca', {}), logger)
        
        # Regime detector (observation-only, no behavioral coupling).
        # Built lazily on first call when vortex_dca.regime_detector.enabled
        # is true. Per-symbol last-compute timestamp throttles log spam to
        # one reading per regime_detector.log_interval_seconds (default
        # 300s = 5 minutes). The detector itself is stateless across
        # invocations — recreating it is cheap if config hot-reloads
        # change thresholds, but we cache to avoid the construction cost
        # on every cycle.
        self._regime_detector: Optional[RegimeDetector] = None
        self._last_regime_compute: Dict[str, float] = {}

        # Take profit tracking
        self.tp_orders = {}  # symbol -> {side: {'order_id': str, 'placed_at': timestamp}}
        self.tp_failed_attempts = {}  # symbol -> {side: timestamp} to prevent spam
        self.tp_refresh_interval = 60  # Refresh TP orders every 60 seconds
        # Per-symbol one-shot recovery flag. On the first TP call after process
        # start (or after an external wipe like a sibling bot's startup
        # cancel_all_orders), self.tp_orders[symbol] is empty but the exchange
        # may still hold reduce-only orders from the previous run. Without
        # recovery every placement is rejected with Bybit 110017
        # ("reduce-only already covered") indefinitely. We adopt orphan
        # reduce-only orders into self.tp_orders on first sight.
        self._tp_recovered: Dict[str, bool] = {}
        
        # Position tracking to detect TP execution
        self.last_positions = {}  # symbol -> {'long': {'qty': float}, 'short': {'qty': float}}
        self.tp_hit_flags = {}  # symbol -> {side: bool} flags to trigger grid refresh
        
        # Tick-vol buffer: per-symbol rolling deque of (ts, mid_price).
        # Used by dynamic_spacing to compute sub-second realised vol from
        # mid-price moves. Reacts faster than ATRP (no candle-close lag).
        from collections import deque
        self._tick_mid_buffer: Dict[str, "deque"] = {}
        self._tick_buffer_maxlen: int = int(
            self.config.get('vortex_dca', {})
            .get('dynamic_spacing', {})
            .get('tick_vol_lookback', 60)
        )
        # EWMA realised-variance state per symbol. When
        # dynamic_spacing.tick_vol_mode='ewma' is set, the tick_vol metric
        # uses exponentially-weighted variance instead of simple std-dev.
        # σ²_t = λ × σ²_(t-1) + (1-λ) × r_t²
        # Reacts FAST to spikes, decays FAST when calm returns. No memory
        # pollution from old ticks.
        self._ewma_tick_var: Dict[str, float] = {}
        self._last_tick_mid: Dict[str, float] = {}

        # Grid refresh tracking
        self.last_grid_refresh = {}  # symbol -> timestamp of last grid refresh
        self.grid_refresh_interval = self.config['vortex_dca'].get('grid_refresh_interval', 180)  # Default 3 minutes
        # Per-symbol set of sides currently locked by refresh_lock. _should_refresh_grid
        # populates this; _refresh_grid / _refresh_grid_dual read it for per-side refresh.
        self._locked_sides_by_symbol = {}
        # Per-(symbol, side) timestamp of when the current staged L1 anchor was
        # established (first time wall_change_skip preserved it). Used to enforce
        # chase_after_secs — after N seconds at the same wall without fill, the
        # staged path switches to frontrun mode (best bid/ask) to chase fills.
        self._staged_anchor_set_at: Dict[str, Dict[str, float]] = {}
        # Per-(symbol, side) timestamp of when the side first became "blocked
        # empty" — flat with no order, filters refusing placement. After
        # chase_after_secs, the bot forces a frontrun placement BYPASSING
        # filters so the side doesn't sit empty forever.
        self._side_blocked_empty_since: Dict[str, Dict[str, float]] = {}
        # Per-symbol set of sides currently in chase mode (force frontrun
        # placement, bypass filters). Set by _wall_change_skip_sides; consumed
        # by calculator via orderbook injection.
        self._force_frontrun_sides: Dict[str, set] = {}

        # Helper orders tracking
        self.helper_state = {}  # symbol -> {'active': bool, 'orders': [], 'placed_at': timestamp, 'side': str}
        self.helper_last_activated = {}  # symbol -> timestamp of last helper activation

        # Auto-hedge tracking (prevents cascading hedges like delta neutral MM)
        vortex_config = self.config.get('vortex_dca', {})
        self.autohedge_enabled = vortex_config.get('vortex_autohedge_enabled', False)
        self.autohedge_ratio = vortex_config.get('vortex_autohedge_ratio', 0.5)
        self.autohedge_on_drawdown_pct = vortex_config.get('vortex_autohedge_on_drawdown_pct', 0.04)
        self.autohedge_on_liquidation_distance_pct = vortex_config.get('vortex_autohedge_on_liquidation_distance_pct', 0.10)
        self.autohedge_tp_target = vortex_config.get('vortex_autohedge_tp_target', 0.002)
        self.autohedge_trailing_enabled = vortex_config.get('vortex_autohedge_trailing_stop_enabled', True)
        self.autohedge_trailing_distance = vortex_config.get('vortex_autohedge_trailing_distance_pct', 0.002)
        if self._is_netting_exchange() and self.autohedge_enabled:
            self.logger.warning(
                "Auto-hedge disabled: exchange is netting-mode, so opposite-side "
                "orders reduce or flip the active position instead of opening a hedge leg"
            )
            self.autohedge_enabled = False
        self.last_hedge_info = {}  # symbol -> {side: {'price': float, 'qty': float, 'timestamp': float, 'original_qty': float}}
        self.hedge_tp_orders = {}  # symbol -> {side: {'order_id': str, 'placed_at': timestamp}}
        self.hedge_best_prices = {}  # symbol -> {side: best_price} - tracks high water mark for trailing

        # Max drawdown stop loss
        self.max_drawdown_usdt = vortex_config.get('max_drawdown_usdt', 0)  # 0 = disabled
        self.stop_cooldown_seconds = vortex_config.get('stop_cooldown_seconds', 300)
        self.stop_loss_timestamps = {}  # symbol -> last stop loss time
        self.stop_loss_counts = {}  # symbol -> consecutive stop count (for escalating cooldown)

        # G3 (multi-symbol): FROZEN per-symbol resolved config cache.
        # symbol -> deep-copied dict(vortex_dca) with symbol_config[symbol]
        # overrides merged on top. Resolved ONCE per symbol (lazily, on first
        # _get_symbol_config call, then cached) so concurrent per-symbol threads
        # never share a nested dict or race an in-place mutation. See
        # _resolve_symbol_config / _get_symbol_config below.
        self._symbol_cfg_cache: Dict[str, Dict] = {}

    def start(self, symbols: List[str]) -> None:
        """Start the strategy for given symbols"""
        self.running = True
        self.logger.info(f"Starting Vortex DCA strategy for {len(symbols)} symbols")
        
        # Store active symbols for reference
        self.active_symbols = set(symbols)
        
        # Log virtual chunking status
        vc_status = self.virtual_chunking.get_status()
        if vc_status['enabled']:
            self.logger.info(f"Virtual chunking enabled: {vc_status['threshold_pct']}% threshold, "
                           f"{vc_status['chunk_count']} chunks, {vc_status['attack_multiplier']}x attack")
        else:
            self.logger.info("Virtual chunking disabled")

        # Log helper status
        helper_config = self.config['vortex_dca']
        if helper_config.get('helper_enabled', False):
            self.logger.info(f"Helper orders enabled: {helper_config.get('helper_wall_size', 5)} orders, "
                           f"{helper_config.get('helper_duration', 60)}s duration, "
                           f"{helper_config.get('helper_multiplier', 1.5)}x multiplier, "
                           f"{helper_config.get('helper_cooldown', 300)}s cooldown")
            self.logger.info(f"⚠️  Helper requires open positions to activate (threshold: {helper_config.get('helper_activation_threshold_qty', 0.0)})")
        else:
            self.logger.info("Helper orders disabled")

        # Log auto-hedge status
        if self.autohedge_enabled:
            self.logger.info(f"🛡️  Auto-hedge ENABLED: {self.autohedge_ratio*100:.0f}% hedge ratio")
            self.logger.info(f"   Triggers: {self.autohedge_on_drawdown_pct*100:.0f}% drawdown OR liq < {self.autohedge_on_liquidation_distance_pct*100:.0f}%")
            self.logger.info(f"   Hedge TP target: {self.autohedge_tp_target*100:.2f}%")
            self.logger.info(f"   ⚠️  Liquidation safeguard DISABLED (replaced by auto-hedge)")
        else:
            self.logger.info("Auto-hedge disabled - using standard liquidation safeguard")

        # Log stop loss status
        if self.max_drawdown_usdt > 0:
            self.logger.info(f"🛑 Max drawdown stop loss ENABLED: ${self.max_drawdown_usdt:.2f} USDT")
            self.logger.info(f"   Cooldown: {self.stop_cooldown_seconds}s base (escalating 2x per consecutive stop, max 1hr)")
        else:
            self.logger.info("Max drawdown stop loss disabled (max_drawdown_usdt=0)")

        # HANDLE REMOVED SYMBOLS: Place TPs for positions not in active list
        self._handle_removed_symbols()
        
        # STARTUP CLEANUP: Cancel all existing orders before starting.
        #
        # G1 (multi-symbol scoping): this block runs ONCE here in start(),
        # BEFORE the per-symbol calculator/thread loop below — never per
        # symbol-spinup — so it cannot nuke a grid a sibling symbol just placed.
        #
        # In a single-symbol process (or a dedicated multi-symbol process that
        # OWNS the whole account) the account-wide cancel_all_orders(symbol=None)
        # is correct: it clears this bot's own stale grids. But if two vortex
        # processes ever share one account's keys, the account-wide nuke would
        # cancel the OTHER process's resting grids. The operator can opt into
        # symbol-scoped cleanup via:
        #   "startup_cleanup_scope": "symbols"   (cancel only configured symbols)
        #   "startup_cleanup_scope": "account"   (default; account-wide nuke)
        # HARD RULE: run ONE multi-symbol vortex process per account. Never a
        # sibling vortex on the same keys. The "symbols" scope is the safety
        # valve for the case where that rule must be broken temporarily.
        cleanup_scope = self.config['vortex_dca'].get('startup_cleanup_scope', 'account')
        cleanup_all = self.config['vortex_dca'].get('startup_cleanup_all_symbols', True)
        if cleanup_scope == 'symbols':
            cleanup_all = False
            self.logger.info(
                "startup_cleanup_scope=symbols → restricting startup cleanup to "
                "configured symbols only (account-wide nuke disabled)"
            )

        if cleanup_all:
            self.logger.info("=== STARTUP CLEANUP: Cancelling ALL orders across entire account ===")
            try:
                # Use the exchange's native cancel_all_orders method (no symbol filter)
                result = self.exchange.cancel_all_orders(symbol=None)
                if result:
                    self.logger.info("✅ Successfully cancelled ALL orders across entire account")
                else:
                    self.logger.warning("❌ Failed to cancel all orders, falling back to manual cleanup")
                    cleanup_all = False  # Fall back to manual cleanup
                    
            except Exception as e:
                self.logger.error(f"Error during global startup cleanup: {e}")
                self.logger.info("Falling back to symbol-specific cleanup...")
                cleanup_all = False  # Fall back to symbol-specific cleanup
        
        if not cleanup_all:
            self.logger.info("=== STARTUP CLEANUP: Cancelling orders for configured symbols only ===")
            for symbol in symbols:
                try:
                    open_orders = self.exchange.get_open_orders(symbol)
                    if open_orders:
                        self.logger.info(f"[{symbol}] Found {len(open_orders)} existing orders, cancelling all...")
                        for order in open_orders:
                            try:
                                self.exchange.cancel_order(order['id'], symbol)
                                self.logger.debug(f"[{symbol}] Cancelled order {order['id']}")
                            except Exception as e:
                                self.logger.warning(f"[{symbol}] Failed to cancel order {order['id']}: {e}")
                        self.logger.info(f"[{symbol}] Cleanup complete")
                    else:
                        self.logger.info(f"[{symbol}] No existing orders to clean up")
                except Exception as e:
                    self.logger.error(f"[{symbol}] Error during startup cleanup: {e}")
        
        self.logger.info("=== STARTUP CLEANUP COMPLETE ===")
        
        # Phase-3 Ticket-3: boot-time wave_queue config validation.
        # Runs once before per-symbol calculator init so the operator sees
        # any schema violations at boot (not at first refresh). The
        # validator reads from config['vortex_dca'] which is where the
        # wave_queue block lives in vortex configs.
        self._validate_wave_queue_config_at_boot()

        for symbol in symbols:
            # Subscribe to real-time WS data if available
            if hasattr(self.exchange, 'subscribe_data'):
                self.exchange.subscribe_data(symbol)
                self.logger.info(f"[{symbol}] Subscribed to real-time WS data")

            # Initialize calculator and lock (pass candle_store for clustering)
            self.calculators[symbol] = VortexDCACalculator(symbol, self.logger, self.candle_store)
            self.locks[symbol] = threading.Lock()
            self.tp_orders[symbol] = {}
            self.tp_failed_attempts[symbol] = {}
            self.last_positions[symbol] = {'long': {'qty': 0}, 'short': {'qty': 0}}
            self.tp_hit_flags[symbol] = {}
            self.last_grid_refresh[symbol] = 0  # Initialize to 0 so first check triggers refresh
            self.helper_state[symbol] = {'active': False, 'orders': [], 'placed_at': 0, 'side': None}
            self.helper_last_activated[symbol] = 0

            # Start thread for this symbol
            thread = threading.Thread(
                target=self._run_symbol,
                args=(symbol,),
                daemon=True
            )
            self.threads[symbol] = thread
            thread.start()
            
            time.sleep(0.1)  # Stagger thread starts
            
    def stop(self) -> None:
        """Stop the strategy"""
        self.running = False
        self.logger.info("Stopping Vortex DCA strategy")

        # Wait for threads to finish
        for symbol, thread in self.threads.items():
            thread.join(timeout=5.0)

    # ------------------------------------------------------------------
    # Phase-3 Ticket-3: boot-time wave_queue config validation
    # ------------------------------------------------------------------
    def _validate_wave_queue_config_at_boot(self) -> None:
        """Validate the ``wave_queue`` block at boot, before any calculator
        is constructed.

        Reads the block from ``self.config['vortex_dca']`` (or top-level
        ``self.config`` as a fallback) and feeds it through
        ``validate_wave_queue_config``. Per design §5:

          - Each returned ERROR is logged as a WARNING with the
            ``[VORTEX_WAVEQUEUE_CONFIG]`` prefix. If ANY ERROR-prefixed
            entry is present, wave_queue is FORCE-DISABLED in-place
            (``self.config['vortex_dca']['wave_queue']['enabled'] = False``)
            so the bot continues on the legacy path rather than crash.
          - Each returned WARN is logged as a WARNING but does NOT force
            disable — the operator is informed; behavior continues.
          - ``NotImplementedError`` from the validator (deferred
            gridspan_progression policies) → log + force disable.

        Safe to call when wave_queue is absent from config (no-op).
        """
        try:
            vortex_cfg = self.config.get('vortex_dca', {}) or {}
        except Exception:
            vortex_cfg = {}
        # Validator reads the 'wave_queue' key directly on the dict it
        # is given. Vortex configs nest wave_queue under vortex_dca.
        try:
            errors = validate_wave_queue_config(vortex_cfg)
        except NotImplementedError as exc:
            self.logger.warning(
                f"[VORTEX_WAVEQUEUE_CONFIG] {exc} — force-disabling "
                f"wave_queue; legacy path will run instead."
            )
            wq = vortex_cfg.get('wave_queue')
            if isinstance(wq, dict):
                wq['enabled'] = False
            return

        if not errors:
            wq = vortex_cfg.get('wave_queue')
            if isinstance(wq, dict) and wq.get('enabled', False):
                # Phase 3.x: auto_max_waves resolution. When the operator
                # opts in, compute max_waves + wave_share from the risk
                # budget + distribution policy and WRITE BACK into the
                # config so downstream readers see resolved values.
                self._resolve_auto_max_waves_at_boot(vortex_cfg, wq)
                self.logger.info(
                    f"[VORTEX_WAVEQUEUE_CONFIG] wave_queue ENABLED — "
                    f"max_waves={wq.get('max_waves')}, "
                    f"wave_share={wq.get('wave_share')}, "
                    f"max_total_exposure_pct={wq.get('max_total_exposure_pct')}, "
                    f"progression={wq.get('gridspan_progression')}"
                )
            return

        has_hard_error = False
        for entry in errors:
            self.logger.warning(f"[VORTEX_WAVEQUEUE_CONFIG] {entry}")
            if entry.startswith("ERROR"):
                has_hard_error = True

        if has_hard_error:
            wq = vortex_cfg.get('wave_queue')
            if isinstance(wq, dict):
                wq['enabled'] = False
                self.logger.warning(
                    f"[VORTEX_WAVEQUEUE_CONFIG] wave_queue.enabled FORCE-SET "
                    f"to False due to config errors above; legacy extension "
                    f"path will run instead."
                )
        else:
            # WARN-only path: wave_queue still enabled; resolve auto if asked.
            wq = vortex_cfg.get('wave_queue')
            if isinstance(wq, dict) and wq.get('enabled', False):
                self._resolve_auto_max_waves_at_boot(vortex_cfg, wq)

    def _resolve_auto_max_waves_at_boot(
        self, vortex_cfg: Dict[str, Any], wq: Dict[str, Any]
    ) -> None:
        """If ``wave_queue.auto_max_waves.enabled``, compute resolved
        ``max_waves`` + ``wave_share`` from the live wallet balance + risk
        budget and write them back into ``wq`` in place.

        No-op (and writes nothing) when:
          - ``auto_max_waves`` block is absent.
          - ``auto_max_waves.enabled`` is False.
          - Wallet balance cannot be fetched (logs WARN, leaves operator
            values intact).
        """
        auto = wq.get("auto_max_waves")
        if not isinstance(auto, dict) or not bool(auto.get("enabled", False)):
            return

        # Fetch live balance for budget math. If the exchange isn't ready
        # yet (boot-race), log a WARN and skip; the operator-set values
        # remain in place.
        try:
            wallet_balance = float(self.exchange.get_balance())
        except Exception as exc:  # noqa: BLE001
            self.logger.warning(
                f"[VORTEX_AUTO_MAX_WAVES] could not fetch wallet balance at "
                f"boot ({exc!r}); leaving operator-set max_waves="
                f"{wq.get('max_waves')!r} / wave_share="
                f"{wq.get('wave_share')!r} unchanged."
            )
            return

        try:
            wallet_exposure_pct = float(vortex_cfg.get("wallet_exposure", 1.0))
            ratio_power = float(vortex_cfg.get("ratio_power", 0.7))
            nr_clusters = int(vortex_cfg.get("nr_clusters", 4))
            min_order_notional_usd = float(
                vortex_cfg.get("min_order_notional_usd", 5.0)
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning(
                f"[VORTEX_AUTO_MAX_WAVES] bad vortex_dca params "
                f"({exc!r}); leaving operator-set values unchanged."
            )
            return

        try:
            resolved_n, resolved_share = resolve_auto_max_waves(
                wave_queue_cfg=wq,
                wallet_balance=wallet_balance,
                wallet_exposure_pct=wallet_exposure_pct,
                ratio_power=ratio_power,
                nr_clusters=nr_clusters,
                min_order_notional_usd=min_order_notional_usd,
                logger=self.logger,
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning(
                f"[VORTEX_AUTO_MAX_WAVES] resolver raised {exc!r}; "
                f"leaving operator-set values unchanged."
            )
            return

        # Write the resolved values back into the config in place so the
        # rest of the pipeline (validator, LazyWaveGenerator, telemetry)
        # reads the resolved numbers.
        wq["max_waves"] = int(resolved_n)
        wq["wave_share"] = list(resolved_share)

    def _run_symbol(self, symbol: str) -> None:
        """Main loop for a single symbol"""
        self.logger.info(f"[{symbol}] Starting Vortex DCA thread")
        
        while self.running:
            try:
                with self.locks[symbol]:
                    self._process_symbol(symbol)
                    
                time.sleep(5.0)  # Process every 5 seconds
                
            except Exception as e:
                self.logger.error(f"[{symbol}] Error in strategy loop: {e}")
                time.sleep(10.0)  # Longer delay on error
                
        self.logger.info(f"[{symbol}] Vortex DCA thread stopped")
        
    def _compute_and_log_regime_if_due(self, symbol: str) -> None:
        """Periodic regime classifier output. Pure observation, no side
        effects on grid / wave_share / spacing / anything.

        Gated on ``vortex_dca.regime_detector.enabled`` (default false).
        When enabled, every ``log_interval_seconds`` (default 300s) we
        fetch the configured candle window via the existing CandleStore
        cache, feed it to :class:`RegimeDetector`, and emit one
        ``[VORTEX_REGIME]`` log line per symbol. Operator validates the
        labels against the chart for a few days; once trusted, Phase 3
        wires this into action gates (asymmetric wave_share, pause
        adverse-side entries on TREND, etc.).

        Any exception is caught and downgraded to a WARNING — the
        regime path must never break the trading loop.
        """
        try:
            rd_cfg = self.config.get('vortex_dca', {}).get('regime_detector') or {}
            if not bool(rd_cfg.get('enabled', False)):
                return
            interval = float(rd_cfg.get('log_interval_seconds', 300))
            now = time.time()
            last = self._last_regime_compute.get(symbol, 0.0)
            if (now - last) < interval:
                return
            # Lazy-build the detector. Construction validates thresholds;
            # if the operator passes invalid config we log once and bail.
            if self._regime_detector is None:
                try:
                    self._regime_detector = RegimeDetector(
                        hurst_window=int(rd_cfg.get('hurst_window', 100)),
                        adx_period=int(rd_cfg.get('adx_period', 14)),
                        hurst_trend_threshold=float(
                            rd_cfg.get('hurst_trend_threshold', 0.55)
                        ),
                        hurst_meanrev_threshold=float(
                            rd_cfg.get('hurst_meanrev_threshold', 0.45)
                        ),
                        adx_trend_threshold=float(
                            rd_cfg.get('adx_trend_threshold', 25.0)
                        ),
                        adx_chop_threshold=float(
                            rd_cfg.get('adx_chop_threshold', 20.0)
                        ),
                    )
                except ValueError as exc:
                    self.logger.warning(
                        f"[VORTEX_REGIME] {symbol} config invalid ({exc!r}); "
                        f"disabling detector for this process"
                    )
                    # Mark "computed" far in the future so we don't retry
                    # the bad-config rebuild on every cycle.
                    self._last_regime_compute[symbol] = now + 3600
                    return

            # Fetch candles via the shared store (5-min cache means
            # multiple symbols hitting it stays cheap).
            tf = str(rd_cfg.get('candle_timeframe', '1h'))
            period = str(rd_cfg.get('candle_period', '1W'))
            candles = self.candle_store.get_candles(
                symbol, timeframe=tf, period_str=period
            )
            self._last_regime_compute[symbol] = now  # record even on failure
            if not candles:
                self.logger.warning(
                    f"[VORTEX_REGIME] {symbol} candle fetch returned empty; "
                    f"skipping this cycle"
                )
                return
            # CandleStore yields dicts. Pass 5-tuples (incl. volume) so
            # the v2 detector applies its volume + range + ATR gates.
            # If the upstream candle source ever drops volume, the
            # detector silently falls back to the v1 Hurst+ADX-only
            # behavior — but every exchange we touch via CCXT does
            # emit volume.
            ohlcv = [
                (
                    c['open'], c['high'], c['low'], c['close'],
                    c.get('volume', 0.0),
                )
                for c in candles
            ]
            reading = self._regime_detector.compute(ohlcv)
            self.logger.info(RegimeDetector.format_reading(symbol, reading))
        except Exception as exc:  # noqa: BLE001
            # Regime path must NEVER break the trading loop.
            self.logger.warning(
                f"[VORTEX_REGIME] {symbol} compute failed (non-fatal): {exc!r}"
            )

    def _resolve_symbol_config(self, symbol: str) -> Dict:
        """Build a FROZEN, deep-copied per-symbol config (G3).

        Starts from a deep copy of the global ``vortex_dca`` block so nested
        dicts (dynamic_spacing, wave_queue, entry_filter, ...) are never shared
        across symbols, then merges ANY keys from ``symbol_config[symbol]`` on
        top — not just ``wallet_exposure``. This lets the operator tune any
        vortex_dca key per symbol. A ``None`` override value is ignored (treated
        as "no override") to preserve the prior wallet_exposure semantics.

        Single-symbol behavior is identical: with no ``symbol_config`` block the
        result is just a private deep copy of ``vortex_dca`` (callers already
        treated the returned dict as private — the old code returned a shallow
        copy and mutated it in place).
        """
        base = copy.deepcopy(self.config.get('vortex_dca', {}))
        symbol_config = self.config.get('symbol_config')
        if (
            symbol_config
            and isinstance(symbol_config, dict)
            and isinstance(symbol_config.get(symbol), dict)
        ):
            overrides = symbol_config[symbol]
            for key, value in overrides.items():
                # Skip comment/meta keys and explicit no-op None overrides.
                if key.startswith('_') or value is None:
                    continue
                old = base.get(key)
                base[key] = copy.deepcopy(value)
                self.logger.info(
                    f"[{symbol}] per-symbol override: {key}={value} "
                    f"(global: {old})"
                )
        return base

    def _get_symbol_config(self, symbol: str) -> Dict:
        """Return the cached frozen per-symbol config (resolved once).

        Backward-compatible signature/return: a dict of ``vortex_dca`` values
        with per-symbol overrides applied. Cached per symbol so the deep copy +
        merge happens once, not every cycle, and so two threads never race a
        shared nested dict.
        """
        try:
            cached = self._symbol_cfg_cache.get(symbol)
            if cached is None:
                cached = self._resolve_symbol_config(symbol)
                self._symbol_cfg_cache[symbol] = cached
            return cached
        except Exception as e:
            self.logger.error(f"[{symbol}] Error getting symbol config: {e}")
            return copy.deepcopy(self.config.get('vortex_dca', {}))

    def _is_netting_exchange(self) -> bool:
        """Return True for exchanges with one net position per market."""
        return bool(getattr(self.exchange, 'is_netting_mode', False))

    def _exchange_symbol_keys(self, symbol: str) -> List[str]:
        """Candidate keys for exchange metadata caches.

        Bybit/BloFin caches are generally keyed by the bot's internal symbols
        like BTCUSDT. Hyperliquid caches are keyed by HL coin names like BTC.
        """
        keys = [symbol]
        normalizer = getattr(self.exchange, '_to_coin', None)
        if callable(normalizer):
            try:
                coin = normalizer(symbol)
                if coin and coin not in keys:
                    keys.append(coin)
            except Exception:
                pass
        return keys

    def _lookup_exchange_cache(self, cache_name: str, symbol: str, default: float) -> float:
        cache = getattr(self.exchange, cache_name, {}) or {}
        for key in self._exchange_symbol_keys(symbol):
            value = cache.get(key)
            if value is None:
                continue
            try:
                value_f = float(value)
            except (TypeError, ValueError):
                continue
            if value_f > 0:
                return value_f
        return default

    def _process_symbol(self, symbol: str) -> None:
        """Process Vortex DCA logic for one symbol"""
        
        # Get current market data
        current_price = self.exchange.get_current_price_cached(symbol)
        if not current_price:
            return
            
        wallet_balance = self.exchange.get_balance()
        positions = self.exchange.get_positions_cached(symbol)
        open_orders = self.exchange.get_open_orders(symbol)

        # MAX DRAWDOWN STOP LOSS: Check before any other logic
        if self.max_drawdown_usdt > 0:
            # Check if in cooldown from previous stop
            if symbol in self.stop_loss_timestamps:
                elapsed = time.time() - self.stop_loss_timestamps[symbol]
                consecutive = self.stop_loss_counts.get(symbol, 1)
                cooldown = min(self.stop_cooldown_seconds * (2 ** (consecutive - 1)), 3600)

                if elapsed < cooldown:
                    # Still in cooldown — log every ~60s (loop runs every 5s)
                    if int(elapsed) % 60 < 6:
                        remaining = cooldown - elapsed
                        self.logger.info(f"[{symbol}] STOP LOSS COOLDOWN: {remaining:.0f}s remaining "
                                        f"(stop #{consecutive}, cooldown {cooldown:.0f}s)")
                    return
                else:
                    # Cooldown expired — reset counter if clean for 1 hour
                    if elapsed > 3600:
                        self.stop_loss_counts[symbol] = 0
                        self.logger.info(f"[{symbol}] Stop loss counter reset (clean for {elapsed/60:.0f} min)")
                    del self.stop_loss_timestamps[symbol]
                    self.logger.info(f"[{symbol}] Stop loss cooldown expired, resuming grid trading")

            # Calculate unrealized PnL across all positions
            unrealized_pnl = self._calculate_unrealized_pnl(positions, current_price)

            if unrealized_pnl < -self.max_drawdown_usdt:
                self.logger.critical(
                    f"[{symbol}] MAX DRAWDOWN STOP LOSS: uPnL ${unrealized_pnl:.2f} "
                    f"exceeds -${self.max_drawdown_usdt:.2f} limit"
                )
                self._execute_stop_loss(symbol, positions, current_price)
                return

        # EMERGENCY CLOSE MONITORING: Check if we have active emergency close orders
        self._monitor_emergency_close_orders(symbol, positions, current_price)

        # AUTO-HEDGE OR LIQUIDATION SAFEGUARD: Choose based on config
        if self.autohedge_enabled:
            # AUTO-HEDGE: Monitor positions and hedge when drawdown/liq triggers
            self._check_auto_hedge(symbol, positions, current_price)

            # TRAILING STOP: Monitor hedges and close with trailing profit
            self._monitor_hedge_trailing_stop(symbol, positions, current_price)
        else:
            # LIQUIDATION SAFEGUARD: Monitor liquidation prices and emergency close if needed
            self._liquidation_safeguard_check(symbol, positions, current_price)

        # REGIME DETECTOR: observation-only, no behavioral change.
        # Periodically log Hurst + ADX regime so the operator can validate
        # the classifier against the live chart before any action gates
        # are wired in (Phase 3). Default-off; opt in via config.
        self._compute_and_log_regime_if_due(symbol)

        # VIRTUAL CHUNKING: Check for stuck positions and trigger recovery
        recovery_needed = self._check_virtual_chunking_recovery(symbol, positions, current_price)
        
        # HEALTH CHECK 1: Ensure TP orders exist for any open positions
        self._health_check_tp_orders(symbol, positions, current_price, open_orders)
        
        # HEALTH CHECK 2: Ensure grid orders exist (recover from manual cancellation)
        grid_health = self._health_check_grid_orders(symbol, positions, open_orders)

        # HELPER ORDERS: Manage active helper orders and activate new ones if conditions met
        self._manage_helper_orders(symbol)

        if self._should_activate_helper(symbol, positions, current_price):
            self._place_helper_orders(symbol, positions, current_price)

        # Check for TP execution (position closed)
        tp_executed = self._check_tp_execution(symbol, positions)
        
        # Check if we should refresh the grid (include health check result)
        should_refresh = self._should_refresh_grid(symbol, current_price, positions, open_orders) or tp_executed or grid_health
        
        if should_refresh:
            # Build per-side position map. The legacy single-position path
            # (active_position) loses dual-position state — when both long
            # AND short are open, only one was treated as "existing" and the
            # other ran the flat-side path (scalper / fresh grid) which
            # blocked on filters. We now pass BOTH sides through so each
            # gets its own rebalanced/extension grid when needed.
            long_pos = None
            short_pos = None
            if positions['long']['qty'] > 0:
                long_pos = {
                    'side': 'long',
                    'size': positions['long']['qty'],
                    'price': positions['long']['entry_price'],
                }
            if positions['short']['qty'] > 0:
                short_pos = {
                    'side': 'short',
                    'size': positions['short']['qty'],
                    'price': positions['short']['entry_price'],
                }

            # Single-position case: identical to before.
            if long_pos and not short_pos:
                self._refresh_grid(symbol, current_price, wallet_balance, long_pos, open_orders)
            elif short_pos and not long_pos:
                self._refresh_grid(symbol, current_price, wallet_balance, short_pos, open_orders)
            elif long_pos and short_pos:
                # Dual-position case: rebalance BOTH sides separately.
                self._refresh_grid_dual(
                    symbol, current_price, wallet_balance,
                    long_pos, short_pos, open_orders,
                )
            else:
                # Flat-flat: original fresh-grid path.
                self._refresh_grid(symbol, current_price, wallet_balance, None, open_orders)
            
        # Handle take profit - check both long and short positions
        if positions['long']['qty'] > 0:
            position = {
                'side': 'long',
                'size': positions['long']['qty'], 
                'price': positions['long']['entry_price']
            }
            self.logger.info(f"[{symbol}] Found long position: {position['size']} @ {position['price']}")
            self._handle_take_profit(symbol, position, current_price)
            
        if positions['short']['qty'] > 0:
            position = {
                'side': 'short',
                'size': positions['short']['qty'],
                'price': positions['short']['entry_price']
            }
            self.logger.info(f"[{symbol}] Found short position: {position['size']} @ {position['price']}")
            self._handle_take_profit(symbol, position, current_price)
            
    def _should_refresh_grid(self, symbol: str, current_price: float, positions: Dict, open_orders: List[Dict]) -> bool:
        """Check if grid should be refreshed"""
        calculator = self.calculators[symbol]
        last_price = self.last_refresh_prices.get(symbol)
        vortex_cfg = self.config['vortex_dca']
        refresh_threshold = vortex_cfg['refresh_threshold']

        # Anchor-stable grid: suppress the price-based refresh threshold.
        # When the grid is anchored at the wave activation price, routine
        # price moves shouldn't force a cancel+replace cycle — the interval
        # refresh + breathing logic handles re-spacing without re-anchoring.
        _wq_srt = vortex_cfg.get('wave_queue') or {}
        _amw_srt = _wq_srt.get('auto_max_waves') or {}
        _anchor_stable_srt = bool(
            _wq_srt.get('enabled', False)
            and _amw_srt.get('enabled', False)
            and _amw_srt.get('anchor_stable_grid', False)
        )
        _suppress_thr = bool(
            _amw_srt.get('suppress_refresh_threshold_when_anchored', True)
        )

        # Check price-based refresh
        if _anchor_stable_srt and _suppress_thr:
            price_refresh = False
        else:
            price_refresh = calculator.should_refresh_grid(current_price, last_price, refresh_threshold)

        # Check interval-based refresh
        current_time = time.time()
        last_refresh_time = self.last_grid_refresh.get(symbol, 0)
        time_since_refresh = current_time - last_refresh_time
        interval_refresh = time_since_refresh >= self.grid_refresh_interval

        if interval_refresh:
            self.logger.info(f"[{symbol}] Grid refresh interval reached ({time_since_refresh:.0f}s >= {self.grid_refresh_interval}s), triggering refresh")

        # ── STAGED RETRY: fast re-anchor for flat sides ──────────────────
        # When staged_initial_entry is on AND at least one side is flat,
        # run a faster retry cadence so L1 follows the orderbook wall
        # without waiting for the full grid_refresh_interval. Per-side
        # refresh logic ensures position-backed sides stay protected.
        staged_enabled = bool(
            (vortex_cfg.get('staged_initial_entry') or {}).get('enabled', False)
        )
        staged_retry_secs = int(
            (vortex_cfg.get('staged_initial_entry') or {}).get('retry_interval_secs', 10)
        )
        flat_side_exists = positions['long']['qty'] == 0 or positions['short']['qty'] == 0
        staged_retry_refresh = (
            staged_enabled
            and flat_side_exists
            and time_since_refresh >= staged_retry_secs
            and not interval_refresh  # don't double-log
        )
        if staged_retry_refresh:
            flat_sides = []
            if positions['long']['qty'] == 0:
                flat_sides.append('long')
            if positions['short']['qty'] == 0:
                flat_sides.append('short')
            self.logger.info(
                f"[{symbol}] Staged retry: {time_since_refresh:.0f}s >= {staged_retry_secs}s, "
                f"flat sides={flat_sides} — re-anchoring L1"
            )

        # Check if position side has no entry orders left
        position_side_empty = False

        # Get non-TP orders (entry orders)
        entry_orders = [order for order in open_orders if not order.get('reduce_only', False)]

        # Check long side
        if positions['long']['qty'] > 0:
            # Has long position - check if no buy orders left
            long_entry_orders = [order for order in entry_orders if order.get('side') == 'buy']
            if len(long_entry_orders) == 0:
                self.logger.info(f"[{symbol}] Long position exists but no long entry orders remaining, triggering refresh")
                position_side_empty = True

        # Check short side
        if positions['short']['qty'] > 0:
            # Has short position - check if no sell orders left
            short_entry_orders = [order for order in entry_orders if order.get('side') == 'sell']
            if len(short_entry_orders) == 0:
                self.logger.info(f"[{symbol}] Short position exists but no short entry orders remaining, triggering refresh")
                position_side_empty = True

        # Direction lock: refuse to move long grid DOWN as price drops,
        # or short grid UP as price rises. Position-empty refreshes always proceed
        # (no orders to keep — must rebuild).
        lock_lower = vortex_cfg.get('refresh_lock_lower', False)
        lock_higher = vortex_cfg.get('refresh_lock_higher', False)
        if (price_refresh or interval_refresh or staged_retry_refresh) and last_price is not None and not position_side_empty:
            # Lock only applies to a side that has BOTH a position AND orders.
            # Sides that are flat (no position) — even if they have stale
            # entry orders sitting on the book — MUST be allowed to refresh
            # so staged_initial_entry / entry_filter can re-anchor those
            # stale orders at fresh walls. The lock's purpose is preserving
            # DCA orders that BACK an existing position; flat-side orders
            # don't qualify.
            long_has_position = positions['long']['qty'] > 0
            short_has_position = positions['short']['qty'] > 0
            long_has_orders = any(o.get('side') == 'buy' for o in entry_orders)
            short_has_orders = any(o.get('side') == 'sell' for o in entry_orders)
            block_long = lock_lower and long_has_position and long_has_orders and current_price < last_price
            block_short = lock_higher and short_has_position and short_has_orders and current_price > last_price
            # Block refresh if ANY side-with-orders would have its grid running away.
            # Priority: keep existing orders to fill > re-anchor opportunistically.
            # A side without orders doesn't contribute to the lock, so it can still
            # get its entry_filter evaluated next refresh.
            sides_blocked = []
            if block_long:
                sides_blocked.append('long')
            if block_short:
                sides_blocked.append('short')
            # Stash which sides are locked so _refresh_grid / _refresh_grid_dual
            # can do per-side refresh (preserve locked-side orders, re-anchor
            # unlocked-side orders). Without per-side, the lock would block
            # the entire refresh and starve flat-side staged entries.
            self._locked_sides_by_symbol[symbol] = set(sides_blocked)
            if sides_blocked:
                drift_pct = abs(current_price - last_price) / last_price * 100 if last_price else 0
                direction = 'down' if current_price < last_price else 'up'
                # Decide whether to block the whole refresh OR allow per-side.
                # Per-side path: if ALL active sides with orders are locked,
                # nothing useful would refresh anyway → block fully. Otherwise
                # let the refresh proceed and per-side logic handles the locked
                # sides as no-ops.
                active_unlocked_sides_with_work = (
                    (long_has_orders or positions['long']['qty'] == 0) and 'long' not in sides_blocked
                ) or (
                    (short_has_orders or positions['short']['qty'] == 0) and 'short' not in sides_blocked
                )
                if not active_unlocked_sides_with_work:
                    self.logger.info(
                        f"[{symbol}] Refresh BLOCKED by direction lock "
                        f"(price {direction} {drift_pct:.2f}%, last={last_price}, now={current_price}, "
                        f"locked_sides={sides_blocked}) — keeping existing grid to fill"
                    )
                    price_refresh = False
                    interval_refresh = False
                    staged_retry_refresh = False
                    # Bump interval timer so we don't re-log every cycle
                    self.last_grid_refresh[symbol] = current_time
                else:
                    self.logger.info(
                        f"[{symbol}] Per-side refresh: locked_sides={sides_blocked} "
                        f"(orders preserved); other sides will re-anchor"
                    )
        else:
            self._locked_sides_by_symbol[symbol] = set()

        return price_refresh or interval_refresh or position_side_empty or staged_retry_refresh
    
    def _check_tp_execution(self, symbol: str, current_positions: Dict) -> bool:
        """Check if a TP order was executed (position closed)"""
        last_pos = self.last_positions[symbol]
        tp_executed = False
        
        # Check for TP hit flags (set when TP order disappears)
        if 'long' in self.tp_hit_flags[symbol]:
            self.logger.info(f"[{symbol}] Long TP hit flag detected, triggering grid refresh")
            tp_executed = True
            del self.tp_hit_flags[symbol]['long']
            # Clean up TP tracking
            if 'long' in self.tp_orders[symbol]:
                del self.tp_orders[symbol]['long']
                
        if 'short' in self.tp_hit_flags[symbol]:
            self.logger.info(f"[{symbol}] Short TP hit flag detected, triggering grid refresh")
            tp_executed = True
            del self.tp_hit_flags[symbol]['short']
            # Clean up TP tracking
            if 'short' in self.tp_orders[symbol]:
                del self.tp_orders[symbol]['short']
        
        # Check long side position change
        if last_pos['long']['qty'] > 0 and current_positions['long']['qty'] == 0:
            self.logger.info(f"[{symbol}] Long position closed (TP executed), triggering grid refresh")
            tp_executed = True
            # Clean up TP tracking for this side
            if 'long' in self.tp_orders[symbol]:
                del self.tp_orders[symbol]['long']
            # Reset persistent DCA plan so next entry rebuilds it
            if symbol in self.calculators:
                self.calculators[symbol].reset_dca_plan('long')

        # Check short side position change
        if last_pos['short']['qty'] > 0 and current_positions['short']['qty'] == 0:
            self.logger.info(f"[{symbol}] Short position closed (TP executed), triggering grid refresh")
            tp_executed = True
            # Clean up TP tracking for this side
            if 'short' in self.tp_orders[symbol]:
                del self.tp_orders[symbol]['short']
            # Reset persistent DCA plan so next entry rebuilds it
            if symbol in self.calculators:
                self.calculators[symbol].reset_dca_plan('short')
        
        # Additional safeguard: if we have no positions but had positions before, ensure refresh
        had_positions = last_pos['long']['qty'] > 0 or last_pos['short']['qty'] > 0
        has_positions = current_positions['long']['qty'] > 0 or current_positions['short']['qty'] > 0
        if had_positions and not has_positions:
            self.logger.info(f"[{symbol}] All positions closed, ensuring grid refresh")
            tp_executed = True
        
        # Update position tracking
        self.last_positions[symbol] = {
            'long': {'qty': current_positions['long']['qty']},
            'short': {'qty': current_positions['short']['qty']}
        }
        
        return tp_executed
    
    def _health_check_tp_orders(self, symbol: str, positions: Dict, current_price: float, open_orders: List[Dict]) -> None:
        """Health check: Ensure TP orders exist for all open positions"""
        
        # Check if symbol is in blacklist (only place TP for blacklisted symbols)
        tp_blacklist = self.config['vortex_dca'].get('tp_blacklist', [])
        if tp_blacklist and symbol not in tp_blacklist:
            return
        
        # Check long position
        if positions['long']['qty'] > 0:
            # Check if TP order exists for long position
            tp_exists = any(
                order.get('reduce_only', False) and 
                order.get('side') == 'sell'  # Long TP is a sell order
                for order in open_orders
            )
            
            if not tp_exists:
                self.logger.warning(f"[{symbol}] HEALTH CHECK: Missing TP for long position, placing now...")
                position = {
                    'side': 'long',
                    'size': positions['long']['qty'],
                    'price': positions['long']['entry_price']
                }
                self._place_take_profit_order(symbol, position, current_price)
        
        # Check short position
        if positions['short']['qty'] > 0:
            # Check if TP order exists for short position
            tp_exists = any(
                order.get('reduce_only', False) and 
                order.get('side') == 'buy'  # Short TP is a buy order
                for order in open_orders
            )
            
            if not tp_exists:
                self.logger.warning(f"[{symbol}] HEALTH CHECK: Missing TP for short position, placing now...")
                position = {
                    'side': 'short',
                    'size': positions['short']['qty'],
                    'price': positions['short']['entry_price']
                }
                self._place_take_profit_order(symbol, position, current_price)
    
    def _health_check_grid_orders(self, symbol: str, positions: Dict, open_orders: List[Dict]) -> bool:
        """Health check: Ensure grid orders exist (recover from manual cancellation)"""
        
        # Get non-TP orders (entry orders)
        entry_orders = [order for order in open_orders if not order.get('reduce_only', False)]
        
        # Expected minimum grid orders based on config
        expected_min_orders = self.config['vortex_dca']['nr_clusters']
        
        # If we have no position and very few entry orders, grid was likely cancelled manually
        no_positions = positions['long']['qty'] == 0 and positions['short']['qty'] == 0
        
        if no_positions and len(entry_orders) < expected_min_orders:
            self.logger.warning(f"[{symbol}] HEALTH CHECK: Grid orders missing ({len(entry_orders)}/{expected_min_orders}), triggering refresh...")
            return True  # Trigger grid refresh
        
        # If we have a position but no entry orders on that side, grid needs refresh.
        #
        # Wave-queue suppression: when wave_queue + auto_max_waves are on
        # AND a side's position cost is at/above the wave-queue hard_cap,
        # NO orders are SUPPOSED to be on the book for that side — the
        # wave-queue correctly refuses to deploy more leverage. HEALTH
        # CHECK without this knowledge keeps triggering refresh every
        # cycle, which causes the OTHER side's orders to be cancelled +
        # re-placed every refresh (grid jitter — never sits long enough
        # to fill). Track refresh attempts per side; back off when the
        # wave-queue is refusing.
        if not hasattr(self, '_health_check_no_op_until'):
            self._health_check_no_op_until = {}
        _now = time.time()
        _vortex_cfg = self.config.get('vortex_dca') or {}
        _wq_cfg_hc = _vortex_cfg.get('wave_queue') or {}
        _amw_cfg_hc = _wq_cfg_hc.get('auto_max_waves') or {}
        _wave_aware_hc = (
            bool(_wq_cfg_hc.get('enabled', False))
            and bool(_amw_cfg_hc.get('enabled', False))
        )

        def _at_hard_cap(side_key: str) -> bool:
            """Return True iff wave-queue would refuse to deploy more on this side."""
            if not _wave_aware_hc:
                return False
            try:
                pos = positions.get(side_key, {})
                pos_qty = float(pos.get('qty', 0))
                pos_price = float(pos.get('entry_price', 0))
                if pos_qty <= 0 or pos_price <= 0:
                    return False
                # Conservative wallet estimate — use the calculator's last
                # known balance if available; otherwise fall back to position
                # cost itself as the floor.
                calc = self.calculators.get(symbol)
                wallet = getattr(calc, '_last_wallet_balance', 0.0) if calc else 0.0
                if wallet <= 0:
                    return False
                wallet_exposure_pct = float(_vortex_cfg.get('wallet_exposure', 100.0))
                max_exposure = wallet * (wallet_exposure_pct / 100.0)
                max_total_pct = float(_wq_cfg_hc.get('max_total_exposure_pct', 1.5))
                hard_cap = max_exposure * max_total_pct
                position_cost = pos_qty * pos_price
                return position_cost >= hard_cap * 0.98  # 2% slack
            except Exception:  # noqa: BLE001
                return False

        def _health_cooldown_active(side_key: str) -> bool:
            return _now < self._health_check_no_op_until.get((symbol, side_key), 0)

        def _arm_cooldown(side_key: str, secs: int = 60) -> None:
            self._health_check_no_op_until[(symbol, side_key)] = _now + secs

        if positions['long']['qty'] > 0:
            long_entry_orders = [o for o in entry_orders if o.get('side') == 'buy']
            if len(long_entry_orders) == 0:
                if _at_hard_cap('long'):
                    if not _health_cooldown_active('long'):
                        self.logger.info(
                            f"[{symbol}] HEALTH CHECK: long at hard_cap "
                            f"(wave-queue would refuse) — suppressing refresh, "
                            f"60s cooldown armed"
                        )
                        _arm_cooldown('long', 60)
                else:
                    self.logger.warning(f"[{symbol}] HEALTH CHECK: Long position exists but no long entry orders, triggering refresh...")
                    return True

        if positions['short']['qty'] > 0:
            short_entry_orders = [o for o in entry_orders if o.get('side') == 'sell']
            if len(short_entry_orders) == 0:
                if _at_hard_cap('short'):
                    if not _health_cooldown_active('short'):
                        self.logger.info(
                            f"[{symbol}] HEALTH CHECK: short at hard_cap "
                            f"(wave-queue would refuse) — suppressing refresh, "
                            f"60s cooldown armed"
                        )
                        _arm_cooldown('short', 60)
                else:
                    self.logger.warning(f"[{symbol}] HEALTH CHECK: Short position exists but no short entry orders, triggering refresh...")
                    return True

        return False  # Grid is healthy
    
    def _monitor_emergency_close_orders(self, symbol: str, positions: Dict, current_price: float) -> None:
        """Monitor and replace emergency close orders until position is closed"""
        
        if not hasattr(self, 'emergency_orders') or symbol not in self.emergency_orders:
            return
            
        emergency_info = self.emergency_orders[symbol]
        side = emergency_info['side']
        
        # Check if position is closed
        if positions[side]['qty'] == 0:
            self.logger.critical(f"[{symbol}] ✅ Emergency close successful - {side} position closed!")
            del self.emergency_orders[symbol]
            return
            
        # Position still exists - check if order needs replacement
        current_time = time.time()
        order_age = current_time - emergency_info['placed_at']
        
        if order_age > 10:  # Replace order every 10 seconds until position closes
            self.logger.critical(f"[{symbol}] Emergency close order aged {order_age:.0f}s, replacing with current price...")
            
            # Cancel old order
            try:
                self.exchange.cancel_order(emergency_info['order_id'], symbol)
                self.logger.info(f"[{symbol}] Cancelled old emergency close order")
            except Exception as e:
                self.logger.warning(f"[{symbol}] Could not cancel old emergency close order: {e}")
            
            # Place new emergency close order
            order_side = 'sell' if side == 'long' else 'buy'
            result = self.exchange._place_order_with_position_side(
                symbol=symbol,
                side=order_side,
                amount=emergency_info['quantity'],
                price=current_price,
                order_type='limit',
                reduce_only=True,
                position_side=side
            )
            
            if result:
                self.logger.critical(f"[{symbol}] ✅ Replaced emergency close order: {result['id']} at {current_price}")
                emergency_info['order_id'] = result['id']
                emergency_info['placed_at'] = current_time
            else:
                self.logger.critical(f"[{symbol}] ❌ Failed to replace emergency close order!")
    
    def _get_priority_symbol_for_virtual_chunking(self) -> Optional[str]:
        """Determine which symbol gets virtual chunking priority during flash crash"""

        flashcrash_enabled = self.config['vortex_dca'].get('flashcrash_protection', False)
        if not flashcrash_enabled:
            return None  # All symbols get virtual chunking

        priority_mode = self.config['vortex_dca'].get('flashcrash_priority', 'largest_loss')
        vc_threshold = self.config['vortex_dca'].get('virtual_chunking_threshold_pct', 3.0)

        # Build list of underwater symbols that would trigger virtual chunking
        underwater_symbols = []

        for symbol in self.active_symbols:
            try:
                positions = self.exchange.get_positions_cached(symbol)
                current_price = self.exchange.get_current_price_cached(symbol)

                if not current_price:
                    continue

                # Check both long and short positions
                for side in ['long', 'short']:
                    if positions[side]['qty'] > 0:
                        entry_price = positions[side]['entry_price']
                        qty = positions[side]['qty']

                        # Calculate unrealized loss %
                        if side == 'long':
                            loss_pct = ((current_price - entry_price) / entry_price) * 100
                        else:  # short
                            loss_pct = ((entry_price - current_price) / entry_price) * 100

                        # Only include symbols that would trigger virtual chunking
                        if loss_pct < -vc_threshold:
                            # Calculate additional metrics
                            position_value = qty * current_price

                            # Calculate distance from liquidation (if available)
                            liq_price = positions[side]['liq_price']
                            if liq_price > 0:
                                if side == 'long':
                                    distance_from_liq = ((current_price - liq_price) / current_price) * 100
                                else:
                                    distance_from_liq = ((liq_price - current_price) / current_price) * 100
                            else:
                                distance_from_liq = 999.0  # Unknown, assume safe

                            underwater_symbols.append({
                                'symbol': symbol,
                                'side': side,
                                'distance_from_liq': distance_from_liq,
                                'loss_pct': loss_pct,
                                'position_value': position_value,
                                'qty': qty
                            })

            except Exception as e:
                self.logger.debug(f"[{symbol}] Error calculating virtual chunking priority: {e}")
                continue

        if not underwater_symbols:
            return None

        # Select priority symbol based on mode
        if priority_mode == "highest_risk":
            # Closest to liquidation
            priority = min(underwater_symbols, key=lambda x: x['distance_from_liq'])
        elif priority_mode == "largest_position":
            # Largest position value
            priority = max(underwater_symbols, key=lambda x: x['position_value'])
        else:  # "largest_loss"
            # Biggest unrealized loss percentage (most negative)
            priority = min(underwater_symbols, key=lambda x: x['loss_pct'])

        self.logger.info(f"🎯 VIRTUAL CHUNKING PRIORITY: {priority['symbol']} ({priority_mode}) "
                        f"- {priority['loss_pct']:.1f}% loss, "
                        f"${priority['position_value']:.0f} position")

        return priority['symbol']

    def _liquidation_safeguard_check(self, symbol: str, positions: Dict, current_price: float) -> None:
        """Monitor liquidation prices and emergency close positions if too close"""

        if not self.config['vortex_dca'].get('liquidation_safeguard', False):
            return  # Safeguard disabled

        safeguard_pct = self.config['vortex_dca'].get('liquidation_safeguard_pct_dist', 50.0)

        # Check if this is BloFin exchange (known issue with same liquidation prices)
        is_blofin = self.config.get('exchange', {}).get('name', '').lower() == 'blofin'

        # BloFin-specific validation: Check if both positions have the same liquidation price
        if is_blofin and positions['long']['qty'] > 0 and positions['short']['qty'] > 0:
            long_liq = positions['long']['liq_price']
            short_liq = positions['short']['liq_price']

            if long_liq > 0 and short_liq > 0 and abs(long_liq - short_liq) < 0.0001:
                self.logger.warning(f"⚠️  [BloFin] Detected identical liquidation prices for both positions "
                                  f"(Long: {long_liq:.4f}, Short: {short_liq:.4f}). "
                                  f"This is a known BloFin API issue - applying additional validation.")

        # Check long position
        if positions['long']['qty'] > 0:
            long_liq = positions['long']['liq_price']
            long_entry = positions['long']['entry_price']
            long_qty = positions['long']['qty']

            self.logger.info(f"[{symbol}] Long position: {long_qty:.1f} @ {long_entry:.4f}, "
                           f"Current: {current_price:.4f}, Liquidation: {long_liq:.4f}")

            if long_liq > 0:  # Valid liquidation price
                # Sanity check: For longs, liquidation price should be below entry price
                if is_blofin and long_liq > long_entry:
                    self.logger.warning(f"⚠️  [BloFin] Invalid liquidation price for long position "
                                      f"(liq {long_liq:.4f} > entry {long_entry:.4f}). "
                                      f"Skipping safeguard due to unreliable data.")
                else:
                    # Calculate how close current price is to liquidation price
                    distance_from_liquidation = ((current_price - long_liq) / current_price) * 100

                    self.logger.info(f"[{symbol}] Long position {distance_from_liquidation:.1f}% from liquidation "
                                   f"(trigger at {safeguard_pct:.1f}%)")

                    if distance_from_liquidation <= safeguard_pct and distance_from_liquidation > 0:
                        self.logger.critical(f"🚨 LIQUIDATION SAFEGUARD TRIGGERED! {symbol} Long position "
                                           f"only {distance_from_liquidation:.1f}% from liquidation price! Emergency closing...")
                        self._emergency_close_position(symbol, 'long', long_qty)
            else:
                self.logger.warning(f"[{symbol}] No valid liquidation price (liq_price={long_liq}), skipping safeguard check")

        # Check short position
        if positions['short']['qty'] > 0:
            short_liq = positions['short']['liq_price']
            short_entry = positions['short']['entry_price']
            short_qty = positions['short']['qty']

            self.logger.info(f"[{symbol}] Short position: {short_qty:.1f} @ {short_entry:.4f}, "
                           f"Current: {current_price:.4f}, Liquidation: {short_liq:.4f}")

            if short_liq > 0:  # Valid liquidation price
                # Sanity check: For shorts, liquidation price should be above entry price
                if is_blofin and short_liq < short_entry:
                    self.logger.warning(f"⚠️  [BloFin] Invalid liquidation price for short position "
                                      f"(liq {short_liq:.4f} < entry {short_entry:.4f}). "
                                      f"Skipping safeguard due to unreliable data.")
                else:
                    # Calculate how close current price is to liquidation price
                    # For shorts: liquidation is above current price
                    distance_from_liquidation = ((short_liq - current_price) / current_price) * 100

                    # Additional validation for BloFin: Skip if distance is unrealistic (>1000%)
                    if is_blofin and distance_from_liquidation > 1000:
                        self.logger.warning(f"⚠️  [BloFin] Unrealistic liquidation distance ({distance_from_liquidation:.1f}%). "
                                          f"Skipping safeguard due to likely data error.")
                    else:
                        self.logger.info(f"[{symbol}] Short position {distance_from_liquidation:.1f}% from liquidation "
                                       f"(trigger at {safeguard_pct:.1f}%)")

                        if distance_from_liquidation <= safeguard_pct and distance_from_liquidation > 0:
                            self.logger.critical(f"🚨 LIQUIDATION SAFEGUARD TRIGGERED! {symbol} Short position "
                                               f"only {distance_from_liquidation:.1f}% from liquidation price! Emergency closing...")
                            self._emergency_close_position(symbol, 'short', short_qty)
            else:
                self.logger.warning(f"[{symbol}] No valid liquidation price (liq_price={short_liq}), skipping safeguard check")
    
    def _check_virtual_chunking_recovery(self, symbol: str, positions: Dict, current_price: float) -> bool:
        """Check if virtual chunking recovery should be triggered for stuck positions"""

        recovery_triggers = self.virtual_chunking.should_trigger_recovery(symbol, positions, current_price)
        recovery_needed = False

        # Check if flashcrash protection is enabled
        flashcrash_enabled = self.config['vortex_dca'].get('flashcrash_protection', False)

        if flashcrash_enabled and (recovery_triggers['long'] or recovery_triggers['short']):
            # Get the priority symbol for virtual chunking
            priority_symbol = self._get_priority_symbol_for_virtual_chunking()

            # If this symbol is not the priority, skip virtual chunking
            if priority_symbol and symbol != priority_symbol:
                self.logger.info(f"[{symbol}] Skipping virtual chunking (priority: {priority_symbol})")
                return False
            elif priority_symbol and symbol == priority_symbol:
                self.logger.critical(f"[{symbol}] 🎯 PRIORITY SYMBOL for virtual chunking - proceeding with recovery")

        # Handle long position recovery
        if recovery_triggers['long'] and positions['long']['qty'] > 0:
            if not self.virtual_chunking.is_recovery_active(symbol, 'long'):
                self.logger.critical(f"[{symbol}] 🔄 INITIATING VIRTUAL CHUNKING RECOVERY for LONG position")
                self._initiate_virtual_chunking_recovery(
                    symbol, positions['long'], current_price, 'long'
                )
                recovery_needed = True

        # Handle short position recovery
        if recovery_triggers['short'] and positions['short']['qty'] > 0:
            if not self.virtual_chunking.is_recovery_active(symbol, 'short'):
                self.logger.critical(f"[{symbol}] 🔄 INITIATING VIRTUAL CHUNKING RECOVERY for SHORT position")
                self._initiate_virtual_chunking_recovery(
                    symbol, positions['short'], current_price, 'short'
                )
                recovery_needed = True
        
        return recovery_needed
    
    def _initiate_virtual_chunking_recovery(self, symbol: str, position: Dict, current_price: float, side: str) -> None:
        """Initiate virtual chunking recovery for a stuck position"""
        try:
            position_qty = position['qty']
            entry_price = position['entry_price']
            
            # Calculate recovery orders
            recovery_orders = self.virtual_chunking.calculate_recovery_orders(
                symbol, position_qty, entry_price, current_price, side
            )

            self.logger.info(f"[{symbol}] Virtual chunking calculated {len(recovery_orders)} recovery orders")
            for price, qty, order_type in recovery_orders:
                self.logger.info(f"[{symbol}] Recovery order: {order_type} {qty} @ {price}")

            # Place recovery orders
            for price, quantity, order_type in recovery_orders:
                if order_type.endswith('_reduce'):
                    # Reduce-only order
                    order_side = order_type.split('_')[0]
                    result = self.exchange._place_order_with_position_side(
                        symbol=symbol,
                        side=order_side,
                        amount=quantity,
                        price=price,
                        order_type='limit',
                        reduce_only=True,
                        position_side=side
                    )
                    if result:
                        self.logger.info(f"[{symbol}] Virtual chunking exit order placed: {order_side} {quantity} @ {price}")
                    else:
                        self.logger.error(f"[{symbol}] Failed to place virtual chunking exit order")
                else:
                    # Regular order (attack order)
                    result = self.exchange.place_order(
                        symbol=symbol,
                        side=order_type,
                        amount=quantity,
                        price=price,
                        order_type='limit'
                    )
                    if result:
                        self.logger.info(f"[{symbol}] Virtual chunking attack order placed: {order_type} {quantity} @ {price}")
                        # Update recovery state
                        self.virtual_chunking.update_recovery_state(symbol, side, 'attacking', {
                            'attack_order_id': result['id'],
                            'target_qty': position_qty,
                            'chunk_size': quantity
                        })
                    else:
                        self.logger.error(f"[{symbol}] Failed to place virtual chunking attack order")
                
                time.sleep(0.1)  # Small delay between orders
                
        except Exception as e:
            self.logger.error(f"[{symbol}] Error initiating virtual chunking recovery: {e}")
    
    def _emergency_close_position(self, symbol: str, side: str, quantity: float) -> None:
        """Emergency close position using market order"""
        try:
            # Determine order side (opposite of position side)
            order_side = 'sell' if side == 'long' else 'buy'
            
            self.logger.critical(f"[{symbol}] EMERGENCY CLOSE: {order_side} {quantity} {symbol} at MARKET")
            
            # Cancel all existing orders first to free up margin
            try:
                open_orders = self.exchange.get_open_orders(symbol)
                for order in open_orders:
                    self.exchange.cancel_order(order['id'], symbol)
                    self.logger.info(f"[{symbol}] Cancelled order {order['id']} for emergency close")
            except Exception as e:
                self.logger.warning(f"[{symbol}] Error cancelling orders during emergency close: {e}")
            
            # Place reduce-only limit order at current market price to close position
            current_price = self.exchange.get_current_price_cached(symbol)
            if not current_price:
                self.logger.critical(f"[{symbol}] Failed to get current price for emergency close")
                return
                
            result = self.exchange._place_order_with_position_side(
                symbol=symbol,
                side=order_side,
                amount=quantity,
                price=current_price,  # At current market price
                order_type='limit',
                reduce_only=True,
                position_side=side
            )
            
            if result:
                self.logger.critical(f"[{symbol}] ✅ EMERGENCY CLOSE ORDER PLACED: {result['id']} at {current_price}")
                
                # Store emergency close order for monitoring/replacement in main loop
                if not hasattr(self, 'emergency_orders'):
                    self.emergency_orders = {}
                self.emergency_orders[symbol] = {
                    'order_id': result['id'],
                    'side': side,
                    'quantity': quantity,
                    'placed_at': time.time()
                }
                
                # Clear TP tracking since position will be closed
                if side in self.tp_orders[symbol]:
                    del self.tp_orders[symbol][side]
            else:
                self.logger.critical(f"[{symbol}] ❌ FAILED to place emergency close order!")
                
        except Exception as e:
            self.logger.critical(f"[{symbol}] 💥 CRITICAL ERROR during emergency close: {e}")
            # This is a critical failure - position could be liquidated
        
    def _calculate_unrealized_pnl(self, positions: Dict, current_price: float) -> float:
        """Calculate total unrealized PnL in USDT across all positions for a symbol"""
        pnl = 0.0

        if positions['long']['qty'] > 0:
            long_entry = positions['long']['entry_price']
            long_qty = positions['long']['qty']
            pnl += (current_price - long_entry) * long_qty

        if positions['short']['qty'] > 0:
            short_entry = positions['short']['entry_price']
            short_qty = positions['short']['qty']
            pnl += (short_entry - current_price) * short_qty

        return pnl

    def _execute_stop_loss(self, symbol: str, positions: Dict, current_price: float) -> None:
        """Execute max drawdown stop loss: cancel orders, market close positions,
        VERIFY closure, then enter cooldown only if positions actually closed.

        If close fails, do NOT escalate cooldown — return early so the next
        loop tick retries instead of locking the bot in 5/10/20/40/60-min waits
        while the position bleeds untouched.
        """
        try:
            long_qty = positions['long']['qty']
            short_qty = positions['short']['qty']
            long_entry = positions['long']['entry_price'] if long_qty > 0 else 0
            short_entry = positions['short']['entry_price'] if short_qty > 0 else 0

            self.logger.critical(f"[{symbol}] === EXECUTING MAX DRAWDOWN STOP LOSS ===")
            self.logger.critical(f"[{symbol}]   Long: {long_qty} @ {long_entry:.6f}")
            self.logger.critical(f"[{symbol}]   Short: {short_qty} @ {short_entry:.6f}")
            self.logger.critical(f"[{symbol}]   Current price: {current_price:.6f}")

            # 1. Cancel ALL orders for this symbol (including TPs that might
            # block reduce-only by holding the entire reducible quantity)
            try:
                open_orders = self.exchange.get_open_orders(symbol)
                cancelled = 0
                for order in open_orders:
                    try:
                        self.exchange.cancel_order(order['id'], symbol)
                        cancelled += 1
                    except Exception as e:
                        self.logger.warning(f"[{symbol}] Failed to cancel order {order['id']}: {e}")
                self.logger.critical(f"[{symbol}]   Cancelled {cancelled}/{len(open_orders)} orders")
                # Brief pause so cancels register before reduce-only fires
                time.sleep(0.5)
            except Exception as e:
                self.logger.error(f"[{symbol}] Error cancelling orders during stop loss: {e}")

            # 2. Re-fetch positions after cancellation (TP could have filled
            # between the snapshot and now, changing actual qty)
            try:
                fresh = self.exchange.get_positions_cached(symbol)
                long_qty_live = fresh.get('long', {}).get('qty', long_qty)
                short_qty_live = fresh.get('short', {}).get('qty', short_qty)
            except Exception as e:
                self.logger.warning(f"[{symbol}] Could not re-fetch positions, using snapshot: {e}")
                long_qty_live = long_qty
                short_qty_live = short_qty

            long_close_ok = (long_qty_live <= 0)
            short_close_ok = (short_qty_live <= 0)

            # 3. Market close long position
            # CRITICAL: pass position_idx=1 for hedge mode. Bybit defaults 0
            # (one-way mode) and silently rejects reduce_only in hedge mode.
            # BloFin maps position_idx=1 → positionSide='long' internally.
            if long_qty_live > 0:
                try:
                    result = self.exchange.place_order(
                        symbol=symbol,
                        side='sell',
                        amount=long_qty_live,
                        price=None,
                        order_type='market',
                        reduce_only=True,
                        position_idx=1,
                    )
                    if result:
                        self.logger.critical(f"[{symbol}]   Closed LONG {long_qty_live} @ market ({result.get('id', 'N/A')})")
                    else:
                        self.logger.critical(f"[{symbol}]   FAILED to close LONG position (place_order returned None)")
                except Exception as e:
                    self.logger.critical(f"[{symbol}]   ERROR closing LONG: {e}")

            # 4. Market close short position (position_idx=2 for hedge-mode short)
            if short_qty_live > 0:
                try:
                    result = self.exchange.place_order(
                        symbol=symbol,
                        side='buy',
                        amount=short_qty_live,
                        price=None,
                        order_type='market',
                        reduce_only=True,
                        position_idx=2,
                    )
                    if result:
                        self.logger.critical(f"[{symbol}]   Closed SHORT {short_qty_live} @ market ({result.get('id', 'N/A')})")
                    else:
                        self.logger.critical(f"[{symbol}]   FAILED to close SHORT position (place_order returned None)")
                except Exception as e:
                    self.logger.critical(f"[{symbol}]   ERROR closing SHORT: {e}")

            # 5. VERIFY: re-fetch positions after market close attempts. Cooldown
            # is set only if positions actually went to zero. Otherwise we
            # return without escalating cooldown so the next loop tick retries.
            time.sleep(0.5)  # let exchange settle
            try:
                verify = self.exchange.get_positions_cached(symbol)
                long_after = verify.get('long', {}).get('qty', long_qty_live)
                short_after = verify.get('short', {}).get('qty', short_qty_live)
            except Exception as e:
                self.logger.error(f"[{symbol}] Could not verify close — assuming success: {e}")
                long_after = 0
                short_after = 0

            long_close_ok = (long_after <= 0)
            short_close_ok = (short_after <= 0)

            if not long_close_ok or not short_close_ok:
                self.logger.critical(
                    f"[{symbol}] === STOP LOSS CLOSE FAILED === "
                    f"long={long_after:.4f} (was {long_qty_live:.4f}), "
                    f"short={short_after:.4f} (was {short_qty_live:.4f}). "
                    f"NOT setting cooldown — will retry on next loop tick."
                )
                return

            # 6. Confirmed flat → set cooldown and clean state
            self.stop_loss_timestamps[symbol] = time.time()
            self.stop_loss_counts[symbol] = self.stop_loss_counts.get(symbol, 0) + 1
            consecutive = self.stop_loss_counts[symbol]
            cooldown = min(self.stop_cooldown_seconds * (2 ** (consecutive - 1)), 3600)

            self.tp_orders[symbol] = {}
            self.tp_failed_attempts[symbol] = {}
            self.last_positions[symbol] = {'long': {'qty': 0}, 'short': {'qty': 0}}
            self.tp_hit_flags[symbol] = {}
            self.helper_state[symbol] = {'active': False, 'orders': [], 'placed_at': 0, 'side': None}
            self.last_refresh_prices.pop(symbol, None)
            self.last_grid_refresh[symbol] = 0

            # Reset persistent DCA plan (both sides) so next entry rebuilds it
            if symbol in self.calculators:
                self.calculators[symbol].reset_dca_plan()

            # Clean up hedge tracking
            self.last_hedge_info.pop(symbol, None)
            self.hedge_tp_orders.pop(symbol, None)
            self.hedge_best_prices.pop(symbol, None)

            # Clean up emergency orders
            if hasattr(self, 'emergency_orders') and symbol in self.emergency_orders:
                del self.emergency_orders[symbol]

            self.logger.critical(
                f"[{symbol}] === STOP LOSS COMPLETE === "
                f"Cooldown: {cooldown:.0f}s (stop #{consecutive})"
            )

        except Exception as e:
            self.logger.critical(f"[{symbol}] CRITICAL ERROR during stop loss execution: {e}")

    def _get_dynamic_spacing_orderbook(
        self,
        symbol: str,
        current_price: float,
        vortex_config: Dict
    ) -> Optional[Dict]:
        """Fetch orderbook and attach price-range volatility when configured."""
        try:
            raw_orderbook = self.exchange.get_orderbook_cached(symbol)
            if not raw_orderbook:
                return None

            orderbook = dict(raw_orderbook)
            self.logger.debug(f"[{symbol}] Fetched orderbook for dynamic spacing")
        except Exception as e:
            self.logger.warning(f"[{symbol}] Failed to fetch orderbook for dynamic spacing: {e}")
            return None

        dynamic_config = vortex_config.get('dynamic_spacing', {})
        volatility_source = str(dynamic_config.get('volatility_source', 'orderbook')).lower()
        if volatility_source not in (
            'price', 'price_range', 'kline', 'klines', 'realized',
            'hybrid', 'max', 'atrp', 'atrp_short', 'atrp_max',
            'tick_vol', 'tick', 'full_stack',
        ):
            return orderbook

        try:
            interval = dynamic_config.get('price_vol_interval', '1m')
            lookback = int(dynamic_config.get('price_vol_lookback', 15))
            lookback = max(2, lookback)
            klines = self.exchange.get_klines(symbol, interval=interval, limit=lookback)
            if not klines:
                self.logger.debug(f"[{symbol}] No klines available for dynamic price volatility")
                return orderbook

            highs = [float(c['high']) for c in klines if c.get('high') is not None]
            lows = [float(c['low']) for c in klines if c.get('low') is not None]
            if not highs or not lows:
                return orderbook

            reference_price = current_price
            if reference_price <= 0:
                closes = [float(c['close']) for c in klines if c.get('close') is not None]
                reference_price = closes[-1] if closes else 0
            if reference_price <= 0:
                return orderbook

            price_range_bps = ((max(highs) - min(lows)) / reference_price) * 10000
            orderbook['_price_volatility_bps'] = max(0.0, price_range_bps)
            orderbook['_price_volatility_interval'] = interval
            orderbook['_price_volatility_lookback'] = lookback

            # ATRP-short — fast-reacting volatility metric.
            # True Range per candle: max(h-l, |h-prev_close|, |l-prev_close|).
            # ATR = mean(TR over last N candles); ATRP = ATR / price in bps.
            # Reacts within minutes to spikes (vs the 15-min range which lags).
            atrp_lookback = int(dynamic_config.get('atrp_short_lookback', 5))
            atrp_lookback = max(2, atrp_lookback)
            if len(klines) >= atrp_lookback + 1:
                recent = klines[-atrp_lookback:]
                prev_closes_seq = [
                    float(klines[-atrp_lookback - 1 + i].get('close', 0) or 0)
                    for i in range(atrp_lookback)
                ]
                tr_sum = 0.0
                tr_count = 0
                for i, c in enumerate(recent):
                    try:
                        h = float(c.get('high', 0) or 0)
                        l = float(c.get('low', 0) or 0)
                        prev_close = prev_closes_seq[i] if i < len(prev_closes_seq) else 0
                        if prev_close <= 0:
                            prev_close = float(c.get('open', 0) or 0) or l
                        tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
                        if tr > 0:
                            tr_sum += tr
                            tr_count += 1
                    except (TypeError, ValueError):
                        continue
                if tr_count > 0:
                    atr = tr_sum / tr_count
                    atrp_bps = (atr / reference_price) * 10000
                    orderbook['_atrp_short_bps'] = max(0.0, atrp_bps)
                    orderbook['_atrp_short_lookback'] = atrp_lookback
        except Exception as e:
            self.logger.debug(f"[{symbol}] Failed to calculate dynamic price volatility: {e}")

        # Tick-vol: rolling mid-price buffer, std-dev of returns × sqrt(N).
        # Reacts in <1 tick (loop_interval ~0.9s). Captures intra-candle
        # spikes before the 1m candle even closes (which is what ATRP needs).
        try:
            from collections import deque
            import time as _time
            best_bid_v = float(orderbook.get('bids', [[0,0]])[0][0]) if orderbook.get('bids') else 0.0
            best_ask_v = float(orderbook.get('asks', [[0,0]])[0][0]) if orderbook.get('asks') else 0.0
            if best_bid_v > 0 and best_ask_v > 0:
                mid_now = (best_bid_v + best_ask_v) / 2.0
                buf = self._tick_mid_buffer.get(symbol)
                if buf is None:
                    buf = deque(maxlen=self._tick_buffer_maxlen)
                    self._tick_mid_buffer[symbol] = buf
                buf.append((_time.time(), mid_now))

                # Choose tick_vol mode: 'std' (default, simple std-dev over
                # buffer) or 'ewma' (exponentially-weighted variance — fast
                # spike reaction + fast decay, no memory pollution).
                tick_vol_mode = str(
                    vortex_config.get('dynamic_spacing', {})
                    .get('tick_vol_mode', 'std')
                ).lower()

                if tick_vol_mode == 'ewma':
                    # EWMA realised variance: σ²_t = λ × σ²_(t-1) + (1-λ) × r_t²
                    # Newer ticks weighted more; old spikes decay away quickly.
                    last_mid = self._last_tick_mid.get(symbol)
                    if last_mid is not None and last_mid > 0:
                        r = (mid_now - last_mid) / last_mid
                        ewma_lambda = float(
                            vortex_config.get('dynamic_spacing', {})
                            .get('ewma_lambda', 0.9)
                        )
                        ewma_lambda = max(0.0, min(0.99, ewma_lambda))
                        prev_var = self._ewma_tick_var.get(symbol, 0.0)
                        new_var = ewma_lambda * prev_var + (1.0 - ewma_lambda) * (r * r)
                        self._ewma_tick_var[symbol] = new_var
                        # Effective lookback ≈ 1/(1-λ). Scale per-tick variance to
                        # "expected move over effective window" so the metric
                        # is comparable to std-mode (both in bps over a window).
                        n_eff = 1.0 / max(1e-6, 1.0 - ewma_lambda)
                        tick_vol_bps = (new_var * n_eff) ** 0.5 * 10000
                        orderbook['_tick_vol_bps'] = max(0.0, tick_vol_bps)
                        orderbook['_tick_vol_lookback'] = int(n_eff)
                    self._last_tick_mid[symbol] = mid_now
                else:
                    # Legacy: std-dev over fixed-size buffer
                    if len(buf) >= 3:
                        mids = [m for _, m in buf]
                        rets = []
                        for i in range(1, len(mids)):
                            if mids[i-1] > 0:
                                rets.append((mids[i] - mids[i-1]) / mids[i-1])
                        if len(rets) >= 2:
                            mean_r = sum(rets) / len(rets)
                            var = sum((r - mean_r) ** 2 for r in rets) / max(1, len(rets) - 1)
                            std = var ** 0.5
                            tick_vol_bps = std * (len(rets) ** 0.5) * 10000
                            orderbook['_tick_vol_bps'] = max(0.0, tick_vol_bps)
                            orderbook['_tick_vol_lookback'] = len(buf)

                # Tick-level synthetic OHLC for sub-1m wick detection.
                # Builds a rolling virtual candle from the last N seconds of
                # mid-price ticks: open=oldest, close=latest, high=max, low=min.
                # Used by wick_catcher.tick_mode to detect rejection wicks in
                # real-time (much faster than waiting for 1m candle close).
                try:
                    wc_cfg_local = (
                        vortex_config.get('staged_initial_entry') or {}
                    ).get('wick_catcher') or {}
                    tm_cfg = wc_cfg_local.get('tick_mode') or {}
                    if tm_cfg.get('enabled', False) and len(buf) >= 5:
                        window_secs = float(tm_cfg.get('window_secs', 30))
                        now = _time.time()
                        recent = [(t, m) for (t, m) in buf if (now - t) <= window_secs]
                        if len(recent) >= 5:
                            mids_recent = [m for _, m in recent]
                            orderbook['_tick_wick_ohlc'] = (
                                float(mids_recent[0]),         # open
                                float(max(mids_recent)),       # high
                                float(min(mids_recent)),       # low
                                float(mids_recent[-1]),        # close
                            )
                            orderbook['_tick_wick_window_n'] = len(recent)
                except Exception as e:
                    self.logger.debug(f"[{symbol}] Failed to calculate tick_wick_ohlc: {e}")
        except Exception as e:
            self.logger.debug(f"[{symbol}] Failed to calculate tick_vol: {e}")

        return orderbook

    def _wall_change_skip_sides(
        self,
        symbol: str,
        current_price: float,
        orderbook: Optional[Dict],
        vortex_config: Dict,
        open_orders: List[Dict],
        positions: Optional[Dict] = None,
    ) -> set:
        """Decide which sides should be skipped because the existing same-side
        top order is within threshold of where a new order would be placed.

        Two paths:
          - STAGED (flat side): compare existing top to the staged wall
            picker output (5-15bps band wall).
          - REBALANCED (position-backed side): compare existing top to the
            would-be rebalanced L1 = current_price ± first_level_distance.

        Either threshold-skip prevents pointless cancel+replace churn when
        prices have barely moved. Wins back queue priority.

        Returns set of {'long','short'} to preserve.
        """
        skip: set = set()
        staged_cfg = vortex_config.get('staged_initial_entry') or {}
        if not staged_cfg.get('enabled', False):
            return skip
        threshold_bps = float(staged_cfg.get('wall_change_threshold_bps', 0))
        if threshold_bps <= 0 or not orderbook:
            return skip
        try:
            from .scalper_entry import pick_initial_entry_price
        except ImportError:
            return skip
        band_cfg = staged_cfg.get('band') or {}
        min_dist = float(band_cfg.get('min_distance_pct', 0.0005))
        max_dist = band_cfg.get('max_distance_pct')
        if max_dist is not None:
            max_dist = float(max_dist)
        price_selection = str(staged_cfg.get('price_selection', 'STRONGEST'))
        threshold_frac = threshold_bps / 10000.0

        # Build existing-order price map per side. Many orders may exist;
        # for skip detection we care about the TOP level (closest to mid).
        entry_orders = [o for o in open_orders if not o.get('reduce_only', False)]
        long_orders = [float(o['price']) for o in entry_orders if o.get('side') == 'buy' and o.get('price')]
        short_orders = [float(o['price']) for o in entry_orders if o.get('side') == 'sell' and o.get('price')]
        existing_top_long = max(long_orders) if long_orders else None     # closest to mid for long
        existing_top_short = min(short_orders) if short_orders else None  # closest to mid for short

        frontrun_enabled = bool(staged_cfg.get('frontrun_fallback', True))
        chase_after_secs = float(staged_cfg.get('chase_after_secs', 0))
        now_ts = time.time()
        anchor_map = self._staged_anchor_set_at.setdefault(symbol, {})
        empty_map = self._side_blocked_empty_since.setdefault(symbol, {})
        force_set = self._force_frontrun_sides.setdefault(symbol, set())
        # Reset chase signals each call; we'll re-arm sides that need it.
        force_set.clear()
        for side, existing_top in (('long', existing_top_long), ('short', existing_top_short)):
            if existing_top is None:
                # No existing order on this side → clear anchor timer.
                # Start (or continue) the "blocked empty" timer if the side
                # has no position either — meaning filters are keeping it
                # flat. After chase_after_secs, force a frontrun placement
                # bypassing filters so the side doesn't stay empty forever.
                anchor_map.pop(side, None)
                # Check if side is truly flat (no position). We can infer this
                # from the absence of any order on that side, BUT the caller
                # may want a more reliable check. For now: if no orders AND
                # chase_after_secs > 0, run the blocked-empty timer.
                if chase_after_secs > 0:
                    empty_since = empty_map.get(side)
                    if empty_since is None:
                        empty_map[side] = now_ts
                    else:
                        empty_age = now_ts - empty_since
                        if empty_age >= chase_after_secs:
                            self.logger.info(
                                f"[{symbol}] {side} BLOCKED-EMPTY CHASE TRIGGERED "
                                f"(side has been empty for {empty_age:.0f}s >= {chase_after_secs:.0f}s) — "
                                f"forcing frontrun placement (bypasses filters)"
                            )
                            force_set.add(side)
                            # Don't reset — let it keep firing until something lands
                continue
            # Side has an existing order → not empty, clear the empty timer
            empty_map.pop(side, None)
            # If this side has a POSITION, it's in rebalanced-grid mode, not
            # staged-entry mode. The staged wall-picker target is the wrong
            # reference here — let the rebalanced-path skip below handle it.
            if positions is not None:
                side_qty = (positions.get(side) or {}).get('qty', 0) or 0
                if side_qty > 0:
                    anchor_map.pop(side, None)
                    continue
            try:
                new_wall = pick_initial_entry_price(
                    orderbook=orderbook,
                    side=side,
                    current_price=current_price,
                    depth=10,
                    nr_bins=5,
                    price_selection=price_selection,
                    max_distance_pct=max_dist,
                    min_distance_pct=min_dist,
                )
            except Exception:  # noqa: BLE001
                new_wall = None
            # If no wall in band, fall through to the spread-edge target so the
            # threshold check still applies to a chase-mode placement.
            target = new_wall
            target_kind = 'wall'
            if target is None and frontrun_enabled:
                try:
                    if side == 'long':
                        target = float(orderbook['bids'][0][0])
                    else:
                        target = float(orderbook['asks'][0][0])
                    target_kind = 'frontrun'
                    if target <= 0:
                        target = None
                except (KeyError, IndexError, TypeError, ValueError):
                    target = None
            if target is None or existing_top <= 0:
                continue
            drift_frac = abs(target - existing_top) / existing_top
            if drift_frac < threshold_frac:
                # Wall hasn't moved meaningfully. Normally we preserve — but
                # check the chase timer first. If the existing order has been
                # at this anchor longer than chase_after_secs, force a
                # cancel+replace at the spread edge to chase the fill.
                anchored_at = anchor_map.get(side)
                if anchored_at is None:
                    # First preservation — start the chase timer
                    anchor_map[side] = now_ts
                    skip.add(side)
                    self.logger.info(
                        f"[{symbol}] {side} staged L1 {target_kind} unchanged "
                        f"(existing={existing_top:.6f}, new={target:.6f}, "
                        f"drift={drift_frac*10000:.2f}bps < {threshold_bps:.0f}bps) — "
                        f"skipping cancel+replace (chase timer started)"
                    )
                else:
                    age = now_ts - anchored_at
                    if chase_after_secs > 0 and age >= chase_after_secs:
                        # Time to migrate to spread edge
                        self.logger.info(
                            f"[{symbol}] {side} staged L1 CHASE TRIGGERED "
                            f"(anchor age {age:.0f}s >= {chase_after_secs:.0f}s) — "
                            f"forcing frontrun placement"
                        )
                        force_set.add(side)
                        # Reset anchor so next placement starts a fresh timer
                        anchor_map.pop(side, None)
                        # DON'T add to skip — let it be cancelled + re-placed
                    else:
                        skip.add(side)
                        self.logger.info(
                            f"[{symbol}] {side} staged L1 {target_kind} unchanged "
                            f"(existing={existing_top:.6f}, new={target:.6f}, "
                            f"drift={drift_frac*10000:.2f}bps < {threshold_bps:.0f}bps, "
                            f"chase age {age:.0f}/{chase_after_secs:.0f}s) — skipping cancel+replace"
                        )
            else:
                # Wall moved meaningfully — placement will happen, reset anchor
                anchor_map.pop(side, None)

        # ── REBALANCED-GRID THRESHOLD SKIP ─────────────────────────────────
        # For sides with a POSITION (rebalanced path, not staged), check the
        # would-be L1 placement against the existing top order. The rebalanced
        # L1 lands at current_price ± first_level_distance. If existing top is
        # within threshold of that, skip the cancel+replace to preserve queue
        # priority and avoid pointless churn every refresh cycle.
        if positions is not None and threshold_frac > 0:
            first_level_distance = float(vortex_config.get('first_level_distance', 0.0008))
            for side, existing_top in (('long', existing_top_long), ('short', existing_top_short)):
                if side in skip:
                    continue  # already decided by staged path
                # Only apply rebalanced check if side has a POSITION
                qty = positions.get(side, {}).get('qty', 0) or 0
                if qty <= 0 or existing_top is None or existing_top <= 0:
                    continue
                if side == 'long':
                    would_be_top = current_price * (1 - first_level_distance)
                else:
                    would_be_top = current_price * (1 + first_level_distance)
                drift_frac = abs(would_be_top - existing_top) / existing_top
                if drift_frac < threshold_frac:
                    self.logger.info(
                        f"[{symbol}] {side} rebalanced L1 unchanged "
                        f"(existing={existing_top:.6f}, would_be={would_be_top:.6f}, "
                        f"drift={drift_frac*10000:.2f}bps < {threshold_bps:.0f}bps) — "
                        f"skipping cancel+replace"
                    )
                    skip.add(side)
        return skip

    def _refresh_grid(
        self,
        symbol: str,
        current_price: float,
        wallet_balance: float,
        position: Optional[Dict],
        open_orders: List[Dict]
    ) -> None:
        """Refresh the DCA grid"""

        calculator = self.calculators[symbol]
        vortex_config = self._get_symbol_config(symbol)
        netting_mode = self._is_netting_exchange()

        self.logger.info(f"[{symbol}] Refreshing Vortex DCA grid at price {current_price}")

        # Fetch orderbook for dynamic spacing (if enabled)
        orderbook = None
        if vortex_config.get('dynamic_spacing', {}).get('enabled', False):
            orderbook = self._get_dynamic_spacing_orderbook(
                symbol=symbol,
                current_price=current_price,
                vortex_config=vortex_config
            )

        # Cancel active helper orders first (if any)
        if self.helper_state[symbol]['active']:
            active_side = self.helper_state[symbol].get('side', 'unknown')
            helper_order_ids = self.helper_state[symbol]['orders']
            self.logger.warning(f"[{symbol}] Grid refresh triggered while helper active, cancelling {len(helper_order_ids)} {active_side} helper orders")

            cancelled_count = 0
            for order_id in helper_order_ids:
                try:
                    self.exchange.cancel_order(order_id, symbol)
                    cancelled_count += 1
                    self.logger.debug(f"[{symbol}] Cancelled helper order {order_id} for grid refresh")
                except Exception as e:
                    self.logger.debug(f"[{symbol}] Helper order {order_id} already gone: {e}")

            self.logger.info(f"[{symbol}] Cancelled {cancelled_count}/{len(helper_order_ids)} {active_side} helper orders for grid refresh")

            # Deactivate helper
            self.helper_state[symbol] = {'active': False, 'orders': [], 'placed_at': 0, 'side': None}
            self.logger.info(f"[{symbol}] Helper deactivated for grid refresh")

        # Per-side refresh: which sides are locked by refresh_lock?
        # Locked sides keep their existing orders (skip cancel + skip placement).
        locked_sides = self._locked_sides_by_symbol.get(symbol, set())
        # Plus: which sides have a staged L1 wall (or rebalanced L1) that
        # hasn't moved enough to justify cancel+replace churn?
        # Build positions dict for the rebalanced-side threshold check.
        positions_for_skip = {
            'long': {'qty': position['size'] if position and position.get('side') == 'long' else 0},
            'short': {'qty': position['size'] if position and position.get('side') == 'short' else 0},
        }
        wall_skip = self._wall_change_skip_sides(
            symbol, current_price, orderbook, vortex_config, open_orders, positions_for_skip,
        )
        skip_sides = locked_sides | wall_skip
        if netting_mode and position:
            active_side = position['side']
            opposite_side = 'short' if active_side == 'long' else 'long'
            if opposite_side in skip_sides:
                self.logger.info(
                    f"[{symbol}] Netting mode: not preserving {opposite_side} "
                    f"orders while {active_side} position is open"
                )
            skip_sides = {side for side in skip_sides if side == active_side}

        # Inject force_frontrun_sides into orderbook so calculate_initial_grid's
        # staged path can override wall-pick with spread-edge placement.
        if orderbook is not None:
            orderbook['_force_frontrun_sides'] = self._force_frontrun_sides.get(symbol, set())

        # COMPUTE-FIRST architecture: build per-side intended placements
        # BEFORE touching the book. Sides that calculator-refuse (return
        # [] from calculate_rebalanced_grid or empty initial-grid output)
        # are added to a dynamic skip set so their existing orders are
        # preserved by _cancel_grid_orders. Static skip set
        # (refresh_lock + wall_change) means "don't even compute" — we
        # treat those sides as preserve-only.
        # Per-side payload: ordered list of (orders, side_label) tuples
        # to place AFTER the cancel call. Sides without a payload are
        # implicitly skipped from cancel (their book orders survive).
        side_payloads: List[Tuple[List[Tuple[float, float]], str]] = []
        computed_sides: set = set()  # sides for which we ran the calculator

        always_both_sides = bool(vortex_config.get('vortex_always_both_sides', False))
        if netting_mode and position and always_both_sides:
            self.logger.info(
                f"[{symbol}] Netting mode: suppressing vortex_always_both_sides "
                "while a position is open"
            )
        effective_always_both_sides = always_both_sides and not netting_mode

        if position and not effective_always_both_sides:
            # Normal behavior: rebalance existing position only
            side = position['side']
            if side in skip_sides:
                reason = "locked by refresh_lock" if side in locked_sides else "wall unchanged"
                self.logger.info(f"[{symbol}] Skipping {side} rebalance — {reason}")
            else:
                computed_sides.add(side)
                new_orders = calculator.calculate_rebalanced_grid(
                    current_price=current_price,
                    current_position_qty=position['size'],
                    current_position_price=position['price'],
                    wallet_balance=wallet_balance,
                    config=vortex_config,
                    side=side,
                    orderbook=orderbook
                )
                if new_orders:
                    side_payloads.append((new_orders, side))
                else:
                    self.logger.info(
                        f"[{symbol}] {side} produced no rebalanced orders — preserving existing {side} orders"
                    )

        else:
            # Either no position OR vortex_always_both_sides=True
            if position and effective_always_both_sides:
                # Have position + always_both_sides: place opposite fresh grid + same side rebalanced grid
                existing_side = position['side']
                opposite_side = 'short' if existing_side == 'long' else 'long'

                # Compute opposite-side fresh grid only if not statically
                # skipped — we don't want to fire the calculator for a
                # side we're contractually going to preserve.
                long_orders: List[Tuple[float, float]] = []
                short_orders: List[Tuple[float, float]] = []
                if opposite_side not in skip_sides:
                    long_orders, short_orders = calculator.calculate_initial_grid(
                        current_price=current_price,
                        wallet_balance=wallet_balance,
                        config=vortex_config,
                        orderbook=orderbook
                    )
                    computed_sides.add(opposite_side)

                # Stage OPPOSITE side fresh grid (if not skipped)
                if opposite_side in skip_sides:
                    reason = "locked by refresh_lock" if opposite_side in locked_sides else "wall unchanged"
                    self.logger.info(f"[{symbol}] Skipping fresh {opposite_side} grid — {reason}")
                elif opposite_side == 'long' and vortex_config['long_mode'] and long_orders:
                    self.logger.info(f"[{symbol}] Staging fresh long grid (opposite to existing {existing_side} position)")
                    side_payloads.append((long_orders, 'long'))
                elif opposite_side == 'short' and vortex_config['short_mode'] and short_orders:
                    self.logger.info(f"[{symbol}] Staging fresh short grid (opposite to existing {existing_side} position)")
                    side_payloads.append((short_orders, 'short'))
                else:
                    # Opposite side computed nothing — fall through, no
                    # payload staged, side will be preserved.
                    self.logger.info(
                        f"[{symbol}] opposite {opposite_side} produced no fresh orders — "
                        f"preserving existing {opposite_side} orders"
                    )

                # Stage rebalanced orders for EXISTING position side (if not skipped)
                if existing_side in skip_sides:
                    reason = "locked by refresh_lock" if existing_side in locked_sides else "wall unchanged"
                    self.logger.info(f"[{symbol}] Skipping {existing_side} rebalance — {reason}")
                else:
                    computed_sides.add(existing_side)
                    rebalanced_orders = calculator.calculate_rebalanced_grid(
                        current_price=current_price,
                        current_position_qty=position['size'],
                        current_position_price=position['price'],
                        wallet_balance=wallet_balance,
                        config=vortex_config,
                        side=existing_side,
                        orderbook=orderbook
                    )
                    if rebalanced_orders:
                        self.logger.info(f"[{symbol}] Adding {len(rebalanced_orders)} rebalanced orders to existing {existing_side} position")
                        side_payloads.append((rebalanced_orders, existing_side))
                    else:
                        self.logger.info(
                            f"[{symbol}] {existing_side} produced no rebalanced orders — "
                            f"preserving existing {existing_side} orders"
                        )

            else:
                # No position: compute initial grid (both sides if enabled, respecting skip_sides)
                long_orders, short_orders = calculator.calculate_initial_grid(
                    current_price=current_price,
                    wallet_balance=wallet_balance,
                    config=vortex_config,
                    orderbook=orderbook
                )

                if 'long' not in skip_sides and vortex_config['long_mode']:
                    computed_sides.add('long')
                    if long_orders:
                        side_payloads.append((long_orders, 'long'))
                    else:
                        self.logger.info(
                            f"[{symbol}] long initial-grid produced no orders — "
                            f"preserving existing long orders (if any)"
                        )

                if 'short' not in skip_sides and vortex_config['short_mode']:
                    computed_sides.add('short')
                    if short_orders:
                        side_payloads.append((short_orders, 'short'))
                    else:
                        self.logger.info(
                            f"[{symbol}] short initial-grid produced no orders — "
                            f"preserving existing short orders (if any)"
                        )

        # Build dynamic skip set:
        #   static skip (refresh_lock + wall_change)
        #   ∪ sides we DIDN'T compute (never intended to touch)
        #   ∪ sides we computed but that refused (no payload)
        payload_sides = {label for _, label in side_payloads}
        all_sides = {'long', 'short'}
        # A side is "preserve" iff:
        #   - statically skipped, OR
        #   - not computed at all (e.g. opposite_side skipped, mode disabled), OR
        #   - computed but produced no orders
        refused_sides = computed_sides - payload_sides
        # Sides we never touched (not computed, not in static skip) — also
        # preserve (e.g. mode disabled, or short side under the "rebalance
        # existing only" branch).
        if netting_mode and position:
            untouched_sides = set()
        else:
            untouched_sides = all_sides - computed_sides - skip_sides
        dynamic_skip = skip_sides | refused_sides | untouched_sides

        if refused_sides:
            self.logger.info(
                f"[{symbol}] refresh: refused sides preserved from cancel: {sorted(refused_sides)}"
            )

        # Anchor-stable per-order tolerance: when the flag is on, instead of
        # cancel-all + place-all per side, we do a per-level diff. Existing
        # orders within ``price_tolerance_bps`` of a target are preserved.
        if self._is_anchor_stable_active(vortex_config):
            handled_sides = self._anchor_stable_refresh(
                symbol=symbol,
                side_payloads=side_payloads,
                vortex_config=vortex_config,
            )
            # For sides handled by anchor-stable diff, ALSO add to skip so
            # the cancel-all pass below doesn't yank the preserved orders.
            dynamic_skip = dynamic_skip | handled_sides
            # Cancel-all only on sides NOT handled by anchor-stable diff.
            self._cancel_grid_orders(symbol, skip_sides=dynamic_skip)
            # Place fresh orders for any side we didn't already diff-place
            # (defense-in-depth: anchor-stable handled empty payload sides
            # are a no-op).
            for orders, label in side_payloads:
                if label in handled_sides:
                    continue
                self._place_grid_orders(symbol, orders, label)
        else:
            # Legacy path: cancel existing non-TP orders for non-skipped sides only.
            self._cancel_grid_orders(symbol, skip_sides=dynamic_skip)
            # Place the staged payloads.
            for orders, label in side_payloads:
                self._place_grid_orders(symbol, orders, label)

        # Update last refresh price and timestamp
        self.last_refresh_prices[symbol] = current_price
        self.last_grid_refresh[symbol] = time.time()

    def _refresh_grid_dual(
        self,
        symbol: str,
        current_price: float,
        wallet_balance: float,
        long_pos: Dict,
        short_pos: Dict,
        open_orders: List[Dict],
    ) -> None:
        """Refresh both sides when BOTH have positions.

        The legacy ``_refresh_grid`` only handled one position at a time —
        with vortex_always_both_sides=true and dual fills, the "other" side
        ran through calculate_initial_grid (scalper-entry / fresh grid path)
        and got soft-blocked by candle/spike filters, so it never received
        rescue extension orders. This path explicitly calls
        calculate_rebalanced_grid for EACH side with its own qty/price so
        both get their proper extension ladder.
        """
        vortex_config = self.config['vortex_dca']
        calculator = self.calculators[symbol]
        orderbook = None
        if vortex_config.get('dynamic_spacing', {}).get('enabled', False):
            try:
                orderbook = self._get_dynamic_spacing_orderbook(
                    symbol=symbol, current_price=current_price, vortex_config=vortex_config,
                )
            except Exception as e:  # noqa: BLE001
                self.logger.warning(f"[{symbol}] dual-refresh: orderbook fetch failed: {e}")

        # Per-side refresh: which sides are locked by refresh_lock?
        locked_sides = self._locked_sides_by_symbol.get(symbol, set())
        # Plus: skip sides whose L1 (staged or rebalanced) hasn't moved enough.
        positions_for_skip = {
            'long': {'qty': long_pos.get('size', 0) if long_pos else 0},
            'short': {'qty': short_pos.get('size', 0) if short_pos else 0},
        }
        wall_skip = self._wall_change_skip_sides(
            symbol, current_price, orderbook, vortex_config, open_orders, positions_for_skip,
        )
        skip_sides = locked_sides | wall_skip

        # Inject force_frontrun_sides into orderbook for the calculator
        if orderbook is not None:
            orderbook['_force_frontrun_sides'] = self._force_frontrun_sides.get(symbol, set())

        # COMPUTE-FIRST architecture: build per-side intended placements
        # BEFORE touching the book. Sides that calculator-refuse (return
        # [] from calculate_rebalanced_grid) are added to a dynamic skip
        # set so their existing orders are preserved by
        # _cancel_grid_orders. Static skip set (refresh_lock +
        # wall_change) means "don't even compute" — preserve as-is.
        side_payloads: List[Tuple[List[Tuple[float, float]], str]] = []
        computed_sides: set = set()
        refused_sides: set = set()
        # Track DCA-plan rebuilds we still need to fire AFTER cancel —
        # this preserves the pre-refactor behaviour where plan rebuild
        # ran inside the per-side loop AFTER (failed) placement.
        plan_rebuild_targets: List[Dict[str, Any]] = []

        for pos in (long_pos, short_pos):
            side_label = pos['side']
            if side_label in skip_sides:
                reason = "locked by refresh_lock" if side_label in locked_sides else "wall unchanged"
                self.logger.info(
                    f"[{symbol}] dual-refresh: skipping {side_label} — {reason}"
                )
                continue
            computed_sides.add(side_label)
            try:
                rebalanced = calculator.calculate_rebalanced_grid(
                    current_price=current_price,
                    current_position_qty=pos['size'],
                    current_position_price=pos['price'],
                    wallet_balance=wallet_balance,
                    config=vortex_config,
                    side=side_label,
                    orderbook=orderbook,
                )
            except Exception as e:
                self.logger.error(
                    f"[{symbol}] dual-refresh: {side_label} rebalanced grid failed: {e}"
                )
                # Treat exception same as refusal — preserve existing orders.
                refused_sides.add(side_label)
                continue
            if rebalanced:
                side_payloads.append((rebalanced, side_label))
            else:
                refused_sides.add(side_label)
                self.logger.info(
                    f"[{symbol}] dual-refresh: {side_label} produced no rebalanced "
                    f"orders — preserving existing {side_label} orders"
                )

            # Stash full-budget DCA plan rebuild requests for after the
            # cancel/place pass — this preserves the original ordering
            # (rebuild happens AFTER attempting to place, regardless of
            # whether placement succeeded).
            if (
                vortex_config.get('persistent_grid_plan', False)
                and not calculator.has_dca_plan(side_label)
            ):
                plan_rebuild_targets.append(pos)

        # Build dynamic skip set: static ∪ refused.
        dynamic_skip = skip_sides | refused_sides
        if refused_sides:
            self.logger.info(
                f"[{symbol}] dual-refresh: refused sides preserved from cancel: "
                f"{sorted(refused_sides)}"
            )

        # Anchor-stable per-order tolerance (mirrors single-position refresh).
        if self._is_anchor_stable_active(vortex_config):
            handled_sides = self._anchor_stable_refresh(
                symbol=symbol,
                side_payloads=side_payloads,
                vortex_config=vortex_config,
            )
            dynamic_skip = dynamic_skip | handled_sides
            self._cancel_grid_orders(symbol, skip_sides=dynamic_skip)
            for rebalanced, side_label in side_payloads:
                if side_label in handled_sides:
                    continue
                self.logger.info(
                    f"[{symbol}] dual-refresh: placing {len(rebalanced)} "
                    f"rebalanced orders for {side_label} position"
                )
                self._place_grid_orders(symbol, rebalanced, side_label)
        else:
            # Cancel existing non-TP grid orders before placing fresh — but
            # preserve orders on skipped + refused sides.
            self._cancel_grid_orders(symbol, skip_sides=dynamic_skip)

            # Place the staged payloads.
            for rebalanced, side_label in side_payloads:
                self.logger.info(
                    f"[{symbol}] dual-refresh: placing {len(rebalanced)} "
                    f"rebalanced orders for {side_label} position"
                )
                self._place_grid_orders(symbol, rebalanced, side_label)

        # Rebuild full-budget DCA plans for sides that need them
        # (post-refactor: runs AFTER cancel/place pass, same as before
        # since the original code's rebuild path didn't depend on the
        # placement outcome).
        for pos in plan_rebuild_targets:
            try:
                full_prices, full_qtys = calculator.build_full_budget_plan(
                    side=pos['side'],
                    current_price=current_price,
                    wallet_balance=wallet_balance,
                    config=vortex_config,
                    orderbook=orderbook,
                )
                if full_prices and full_qtys:
                    calculator.store_dca_plan(pos['side'], full_prices, full_qtys)
                    self.logger.info(
                        f"[{symbol}] dual-refresh: stored full-budget DCA plan "
                        f"for {pos['side']} ({len(full_prices)} levels) — "
                        f"recovered from missing-plan state via full-budget rebuild"
                    )
            except Exception as e:  # noqa: BLE001
                self.logger.error(
                    f"[{symbol}] dual-refresh: full-budget plan rebuild failed "
                    f"for {pos['side']}: {e}"
                )

        self.last_refresh_prices[symbol] = current_price
        self.last_grid_refresh[symbol] = time.time()

    def _quantize_for_exchange(
        self, symbol: str, price: float, qty: float
    ) -> Optional[Tuple[float, float]]:
        """Apply lot-step + min_notional reality check to a single order.

        The calculator emits ideal (price, qty) tuples. Bybit silently rejects
        orders below $5 notional AND rounds qty DOWN to the lot step (1 LAB,
        0.001 BTC, etc.). Without this final-stage check we'd send orders
        that look like $5.00 to the calculator but become $4.51 after the
        exchange rounds 3.32 LAB → 3 LAB.

        Returns ``(qty_quantized, qty_step)`` or ``None`` if no qty satisfies
        both constraints (caller drops the order). Lot step is read from the
        exchange's pre-fetched ``_market_qty_steps`` cache. Min_notional
        defaults to ``vortex_dca.min_order_notional_usd`` (config) or 5.0.
        """
        import math
        if price <= 0 or qty <= 0:
            return None
        config_min_notional = float(
            (self.config.get('vortex_dca') or {}).get('min_order_notional_usd', 5.0)
        )
        exchange_min_notional = self._lookup_exchange_cache(
            '_market_min_order_values', symbol, config_min_notional
        )
        min_notional = max(config_min_notional, exchange_min_notional)
        # Fetch lot step. Bybit's adapter normalizes symbols internally;
        # Hyperliquid caches are keyed by coin (BTC), not the bot's internal
        # symbol (BTCUSDT), so check both forms before falling back.
        qty_step = self._lookup_exchange_cache('_market_qty_steps', symbol, 1.0)
        if qty_step <= 0:
            qty_step = 1.0

        # Round UP to next lot step that satisfies min_notional.
        min_qty_floor = min_notional / price
        target_qty = max(qty, min_qty_floor)
        # ceil(target_qty / qty_step) * qty_step
        n_steps = math.ceil(target_qty / qty_step)
        qty_q = n_steps * qty_step

        # Final notional check: if even ceil-rounded qty is below floor
        # (shouldn't happen but defensively), give up on this level.
        if qty_q * price < min_notional:
            return None
        return qty_q, qty_step

    def _post_only_safe_price(
        self, symbol: str, order_side: str, price: float
    ) -> tuple:
        """Validate a post-only price against the live top-of-book.

        Post-only buys must rest BELOW the current ask. Post-only sells must
        rest ABOVE the current bid. If the requested price would cross, snap
        to the safe-side-of-spread (best_ask − tick for buy, best_bid + tick
        for sell) so the order rests as the new top-of-book on our side
        rather than being rejected.

        Returns (safe_price, snapped: bool). If we can't read the book, falls
        through unchanged.
        """
        try:
            ob = self.exchange.get_orderbook_cached(symbol, limit=1)
            best_bid = float(ob['bids'][0][0]) if ob.get('bids') else None
            best_ask = float(ob['asks'][0][0]) if ob.get('asks') else None
        except Exception:
            return price, False
        tick = self._lookup_exchange_cache('_market_tick_sizes', symbol, 0.0001)
        if order_side == 'buy' and best_ask is not None and price >= best_ask:
            return max(0.0, best_ask - tick), True
        if order_side == 'sell' and best_bid is not None and price <= best_bid:
            return best_bid + tick, True
        return price, False

    def _place_grid_orders(self, symbol: str, orders: List[tuple], side: str) -> None:
        """Place grid orders. Each order is quantized to exchange constraints
        (lot_step + min_notional) AND validated against the live top-of-book
        so post-only doesn't reject for crossing — orders that would cross
        are snapped to the safe edge of the spread (best_ask−tick for buy,
        best_bid+tick for sell). Orders that still can't be made valid are
        dropped with a log line so we don't waste API calls."""
        order_side = 'buy' if side == 'long' else 'sell'

        placed = 0
        dropped = 0
        snapped = 0
        for price, quantity in orders:
            # Step 1: post-only crossing safety. If the calculator's price
            # would lift the ask (buy) or hit the bid (sell), snap to safe.
            safe_price, was_snapped = self._post_only_safe_price(
                symbol, order_side, price,
            )
            if was_snapped:
                self.logger.info(
                    f"[{symbol}] post-only safety: snapped {side} price "
                    f"{price:.8g} → {safe_price:.8g} (would cross spread)"
                )
                price = safe_price
                snapped += 1

            # Step 2: lot_step + min_notional quantization.
            quantized = self._quantize_for_exchange(symbol, price, quantity)
            if quantized is None:
                dropped += 1
                self.logger.info(
                    f"[{symbol}] dropped {side} order: {quantity:.6f} @ {price} — "
                    f"can't satisfy lot_step + min_notional constraints"
                )
                continue
            quantity_q, _step = quantized

            try:
                result = self.exchange.place_order(
                    symbol=symbol,
                    side=order_side,
                    amount=quantity_q,
                    price=price,
                    order_type='limit',
                    post_only=True  # Use maker orders for better fees
                )

                if result:
                    placed += 1
                    self.logger.debug(
                        f"[{symbol}] Placed {side} order: {quantity_q} @ {price} "
                        f"(notional=${quantity_q*price:.2f})"
                    )
                else:
                    self.logger.warning(
                        f"[{symbol}] Failed to place {side} order: {quantity_q} @ {price}"
                    )

                time.sleep(0.05)

            except Exception as e:
                self.logger.error(
                    f"[{symbol}] Error placing {side} order {quantity_q} @ {price}: {e}"
                )

        if placed or dropped or snapped:
            extra = f", {snapped} snapped" if snapped else ""
            self.logger.info(
                f"[{symbol}] {side} order placement summary: "
                f"{placed} placed, {dropped} dropped{extra}"
            )
                
    @staticmethod
    def _is_anchor_stable_active(vortex_config: Dict[str, Any]) -> bool:
        """True when ``wave_queue.auto_max_waves.anchor_stable_grid=true``.

        Guards opt-in usage of the per-order-tolerance refresh path. All
        three layers must be on (wave_queue, auto_max_waves, anchor_stable_grid)
        to flip the new behavior on — same gating as the calculator side.
        """
        wq = (vortex_config or {}).get("wave_queue") or {}
        if not bool(wq.get("enabled", False)):
            return False
        amw = wq.get("auto_max_waves") or {}
        if not bool(amw.get("enabled", False)):
            return False
        return bool(amw.get("anchor_stable_grid", False))

    def _anchor_stable_refresh(
        self,
        symbol: str,
        side_payloads: List[Tuple[List[Tuple[float, float]], str]],
        vortex_config: Dict[str, Any],
    ) -> set:
        """Per-order cancel-and-replace refresh for anchor-stable grids.

        For each (orders, side_label) tuple in ``side_payloads``, fetch
        the live on-book orders for ``side_label``, compute the diff via
        ``_anchor_stable_diff_orders``, cancel only the entries flagged
        for replacement, and place only the new entries needed.

        Returns the set of side labels that were handled here (so the
        caller can extend its cancel-all skip set to preserve them).
        """
        wq = (vortex_config or {}).get("wave_queue") or {}
        amw = wq.get("auto_max_waves") or {}
        price_tolerance_bps = float(amw.get("price_tolerance_bps", 5))
        order_match_band_bps = float(amw.get("order_match_band_bps", 50))

        try:
            open_orders = self.exchange.get_open_orders(symbol)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning(
                f"[{symbol}] anchor-stable refresh: open-orders fetch "
                f"failed ({exc}); falling back to legacy cancel-all path"
            )
            return set()

        handled: set = set()
        for orders, side_label in side_payloads:
            if not orders:
                continue
            preserved, to_cancel, to_place = self._anchor_stable_diff_orders(
                existing_orders=open_orders,
                target_orders=orders,
                side=side_label,
                price_tolerance_bps=price_tolerance_bps,
                order_match_band_bps=order_match_band_bps,
            )
            n_pres = len(preserved)
            n_can = len(to_cancel)
            n_pl = len(to_place)
            n_target = len(orders)
            if n_can == 0 and n_pl == 0:
                self.logger.info(
                    f"[VORTEX_PRESERVE] {symbol} {side_label} all "
                    f"{n_target} levels within {price_tolerance_bps:.0f} "
                    f"bps — no cancel/replace"
                )
            else:
                self.logger.info(
                    f"[VORTEX_PARTIAL_REFRESH] {symbol} {side_label} "
                    f"cancel+replace {n_pl}/{n_target} levels "
                    f"(preserved {n_pres})"
                )
                # Cancel the stale entries.
                for entry in to_cancel:
                    try:
                        self.exchange.cancel_order(entry.get("id"), symbol)
                    except Exception as exc:  # noqa: BLE001
                        self.logger.warning(
                            f"[{symbol}] anchor-stable refresh: cancel "
                            f"failed for {entry.get('id')!r}: {exc}"
                        )
                # Place the fresh entries (subset of orders).
                if to_place:
                    self._place_grid_orders(symbol, to_place, side_label)
            handled.add(side_label)
        return handled

    @staticmethod
    def _anchor_stable_diff_orders(
        existing_orders: List[Dict[str, Any]],
        target_orders: List[Tuple[float, float]],
        side: str,
        price_tolerance_bps: float,
        order_match_band_bps: float,
    ) -> Tuple[List[Dict[str, Any]],
               List[Dict[str, Any]],
               List[Tuple[float, float]]]:
        """Per-order cancel-and-replace diff for the anchor-stable grid path.

        For each target level, find the closest existing on-book order on
        the same side within ``order_match_band_bps``. Decide per-target:

          - within ``price_tolerance_bps`` → preserve existing, no churn.
          - outside ``price_tolerance_bps`` but inside band → cancel
            that one + place fresh at the target.
          - no match within band → place fresh (gap fill).

        Order book entries that don't get assigned to any target end up
        being cancelled too (they belong to a stale grid layout).

        Args:
            existing_orders: list of order dicts as returned by
                ``exchange.get_open_orders``. Must carry ``side``
                (``'buy'``/``'sell'``), ``price``, and ``id``. Entries
                with ``reduce_only=True`` are ignored.
            target_orders: list of ``(price, qty)`` tuples — the new
                anchor-stable grid we want on book.
            side: ``'long'`` or ``'short'``. Matches the exchange
                ``side`` field (``buy`` for long, ``sell`` for short).
            price_tolerance_bps: max distance to count an existing
                order as "good enough to preserve".
            order_match_band_bps: max distance to consider an existing
                order a candidate for matching to a target at all.

        Returns:
            ``(preserved, to_cancel, to_place)`` where:
              - ``preserved`` is the existing order entries to leave alone
              - ``to_cancel`` is the existing entries to cancel
              - ``to_place`` is the (price, qty) tuples to place fresh

        Both bps thresholds are interpreted relative to the TARGET price,
        which keeps the comparison stable across drift.
        """
        # Filter to entry orders matching this side.
        order_side = 'buy' if side == 'long' else 'sell'
        candidates: List[Dict[str, Any]] = []
        for o in existing_orders:
            if o.get('reduce_only', False):
                continue
            if o.get('side') != order_side:
                continue
            try:
                _ = float(o.get('price'))
            except (TypeError, ValueError):
                continue
            candidates.append(o)

        preserved: List[Dict[str, Any]] = []
        to_cancel: List[Dict[str, Any]] = []
        to_place: List[Tuple[float, float]] = []

        # Greedy matching: for each target (in order), pick the closest
        # candidate within the band that hasn't already been matched.
        matched_ids: set = set()
        match_band = float(order_match_band_bps) / 10000.0
        pres_tol = float(price_tolerance_bps) / 10000.0
        for tgt_price, tgt_qty in target_orders:
            if tgt_price <= 0:
                continue
            best = None
            best_dist = None
            for c in candidates:
                if c.get('id') in matched_ids:
                    continue
                try:
                    c_px = float(c.get('price'))
                except (TypeError, ValueError):
                    continue
                if c_px <= 0:
                    continue
                rel = abs(c_px - tgt_price) / tgt_price
                if rel > match_band:
                    continue
                if best is None or rel < best_dist:
                    best = c
                    best_dist = rel
            if best is not None:
                matched_ids.add(best.get('id'))
                if best_dist <= pres_tol:
                    preserved.append(best)
                else:
                    to_cancel.append(best)
                    to_place.append((tgt_price, tgt_qty))
            else:
                # No candidate within band — must place fresh (gap fill).
                to_place.append((tgt_price, tgt_qty))

        # Any candidate not matched to a target is stale → cancel.
        for c in candidates:
            if c.get('id') in matched_ids:
                continue
            to_cancel.append(c)

        return preserved, to_cancel, to_place

    def _cancel_grid_orders(self, symbol: str, skip_sides: Optional[set] = None) -> None:
        """Cancel non-take-profit orders - fetches fresh orders from exchange.

        Args:
            skip_sides: optional set of {'long', 'short'} — orders for those
                sides are preserved (not cancelled). Used to implement
                per-side refresh when refresh_lock fires on one side.
        """
        skip_sides = skip_sides or set()
        try:
            # Fetch FRESH orders from exchange (not stale cached list)
            open_orders = self.exchange.get_open_orders(symbol)
            self.logger.debug(f"[{symbol}] Fetched {len(open_orders)} fresh orders for cancellation")

            cancelled_count = 0
            preserved_count = 0
            for order in open_orders:
                if order.get('reduce_only', False):
                    continue
                order_side = 'long' if order.get('side') == 'buy' else 'short'
                if order_side in skip_sides:
                    preserved_count += 1
                    continue
                try:
                    self.exchange.cancel_order(order['id'], symbol)
                    cancelled_count += 1
                    self.logger.debug(f"[{symbol}] Cancelled grid order {order['id']}")
                except Exception as e:
                    self.logger.warning(f"[{symbol}] Failed to cancel order {order['id']}: {e}")

            extra = f", preserved {preserved_count} for locked sides {skip_sides}" if skip_sides else ""
            self.logger.info(f"[{symbol}] Cancelled {cancelled_count} vortex grid orders{extra}")
        except Exception as e:
            self.logger.error(f"[{symbol}] Error fetching/cancelling grid orders: {e}")
                    
    def _handle_take_profit(self, symbol: str, position: Dict, current_price: float) -> None:
        """Handle take profit for position"""

        side = position['side']
        self.logger.info(f"[{symbol}] _handle_take_profit called for {side} position {position['size']} @ {position['price']}")

        # === ADOPT ORPHAN TPs whenever tracker is empty for this side ===
        # Originally this ran ONCE per symbol per process startup, but a
        # sibling bot's startup wipe (cancel_all_orders(symbol=None)) can
        # desync the tracker after startup: this bot places a fresh TP,
        # the sibling cancels it, this bot's tracker still points at the
        # dead order id, verify path clears tracker, next place attempt
        # races with a just-placed TP that's now an orphan from this bot's
        # POV → Bybit returns 110017 "reduce-only already covered" forever.
        #
        # Self-healing: every cycle, if THIS side's tracker is empty, query
        # the exchange for any reduce-only on the matching side and adopt
        # it. After adoption the verify path takes over normally and
        # placement attempts stop until the 60s refresh cycle.
        #
        # Cost: one extra get_open_orders call per side per cycle when
        # tracker is empty (rare in steady state; frequent only during
        # the post-restart-cascade window this fix is designed to handle).
        # The legacy ``self._tp_recovered`` flag is no longer consulted —
        # the side-keyed tracker check supersedes it but we leave the
        # field defined on the instance for back-compat with any other
        # reader.
        if side not in self.tp_orders[symbol]:
            try:
                import time as _time
                open_orders = self.exchange.get_open_orders(symbol)
                # sell reduce-only closes a LONG; buy reduce-only closes a SHORT.
                wanted_side_str = 'sell' if side == 'long' else 'buy'
                for o in open_orders:
                    if not o.get('reduce_only'):
                        continue
                    if o.get('side', '') != wanted_side_str:
                        continue
                    self.tp_orders[symbol][side] = {
                        'order_id': o['id'],
                        'placed_at': _time.time(),
                    }
                    self.logger.info(
                        f"[{symbol}] RECOVERED ORPHAN TP ({side.upper()}): "
                        f"{o['id']} {wanted_side_str} {o.get('amount', '?')} @ {o.get('price', '?')}"
                    )
                    break  # one orphan per side is enough
            except Exception as e:
                self.logger.warning(f"[{symbol}] TP recovery query failed (non-fatal): {e}")

        # Check if symbol is in blacklist (only place TP for blacklisted symbols)
        tp_blacklist = self.config['vortex_dca'].get('tp_blacklist', [])
        if tp_blacklist and symbol not in tp_blacklist:
            self.logger.info(f"[{symbol}] Symbol not in tp_blacklist, skipping take profit placement")
            return
        
        # Place TP order immediately when position exists (like original directionalscalper)
        self._place_take_profit_order(symbol, position, current_price)
            
    def _place_take_profit_order(self, symbol: str, position: Dict, current_price: float) -> None:
        """Place take profit order"""
        
        side = position['side']
        size = position['size']
        
        self.logger.info(f"[{symbol}] _place_take_profit_order called for {side} {size}")
        
        import time
        current_time = time.time()
        
        # Check if TP order already exists
        if side in self.tp_orders[symbol]:
            tp_info = self.tp_orders[symbol][side]
            tp_order_id = tp_info['order_id']
            placed_at = tp_info['placed_at']
            
            # Check if it's time to refresh the TP order (every 60 seconds)
            if current_time - placed_at > self.tp_refresh_interval:
                self.logger.info(f"[{symbol}] Refreshing {side} TP order (age: {current_time - placed_at:.0f}s)")
                
                # Cancel the old TP order
                try:
                    self.exchange.cancel_order(tp_order_id, symbol)
                    self.logger.info(f"[{symbol}] Cancelled old TP order {tp_order_id}")
                except Exception as e:
                    self.logger.debug(f"[{symbol}] Could not cancel old TP order {tp_order_id}: {e}")
                
                # Remove from tracking so a new one will be placed
                del self.tp_orders[symbol][side]
                
            else:
                # Verify the order actually exists on the exchange
                try:
                    open_orders = self.exchange.get_open_orders(symbol)
                    tp_exists = any(order['id'] == tp_order_id and order.get('reduce_only', False) 
                                  for order in open_orders)
                    
                    if tp_exists:
                        self.logger.info(f"[{symbol}] TP order {tp_order_id} verified active for {side} (age: {current_time - placed_at:.0f}s)")
                        return
                    else:
                        # TP order no longer exists, remove from tracking
                        self.logger.warning(f"[{symbol}] TP order {tp_order_id} no longer exists for {side}, removing from tracking")
                        del self.tp_orders[symbol][side]
                        # Set flag to trigger grid refresh on next cycle
                        self.tp_hit_flags[symbol][side] = True
                        self.logger.info(f"[{symbol}] Setting TP hit flag for {side} to trigger grid refresh")
                        
                except Exception as e:
                    self.logger.error(f"[{symbol}] Error verifying TP order {tp_order_id}: {e}")
                    # If we can't verify, assume it doesn't exist and remove from tracking
                    del self.tp_orders[symbol][side]
                    # Set flag to trigger grid refresh on next cycle
                    self.tp_hit_flags[symbol][side] = True
            
        # Check if we recently failed to place TP order (prevent spam)
        if side in self.tp_failed_attempts[symbol]:
            last_failed = self.tp_failed_attempts[symbol][side]
            if current_time - last_failed < 5:  # Wait 5 seconds before retrying
                self.logger.debug(f"[{symbol}] Recently failed to place {side} TP order, waiting...")
                return
            
        # Calculate TP price
        entry_price = position['price']
        min_tp_pct = self.config['vortex_dca']['minimum_tp']
        
        if side == 'long':
            target_tp_price = entry_price * (1 + min_tp_pct)
            tp_side = 'sell'

            # If current price is already above TP target, chase the best ask
            if current_price > target_tp_price:
                tp_price = current_price  # Place at current price (near best ask) - post_only ensures maker
                self.logger.info(f"[{symbol}] Long TP: price surpassed target, chasing at {tp_price:.8f}")
            else:
                tp_price = target_tp_price
        else:
            target_tp_price = entry_price * (1 - min_tp_pct)
            tp_side = 'buy'

            # If current price is already below TP target, chase the best bid
            if current_price < target_tp_price:
                tp_price = current_price  # Place at current price (near best bid) - post_only ensures maker
                self.logger.info(f"[{symbol}] Short TP: price surpassed target, chasing at {tp_price:.8f}")
            else:
                tp_price = target_tp_price
            
        # Place TP order
        try:
            self.logger.info(f"[{symbol}] Placing TP order: {tp_side} {size} @ {tp_price} (entry: {entry_price}, tp%: {min_tp_pct*100:.2f}%)")
            result = self.exchange.place_take_profit_order(
                symbol=symbol,
                side=tp_side,
                amount=size,
                price=tp_price,
                position_side=side  # Pass the position side for correct positionIdx
            )
            
            if result:
                self.tp_orders[symbol][side] = {
                    'order_id': result['id'],
                    'placed_at': current_time
                }
                self.logger.info(f"[{symbol}] Placed {side} TP order: {size} @ {tp_price} ({min_tp_pct*100:.2f}%)")
                # Clear any failed attempt record on success
                if side in self.tp_failed_attempts[symbol]:
                    del self.tp_failed_attempts[symbol][side]
            else:
                # Mark failed attempt to prevent spam
                self.tp_failed_attempts[symbol][side] = current_time
                self.logger.warning(f"[{symbol}] Failed to place {side} TP order, will retry in 5s")
            
        except Exception as e:
            # Mark failed attempt on exception too
            self.tp_failed_attempts[symbol][side] = current_time
            self.logger.error(f"[{symbol}] Error placing TP order: {e}")
            
    def _should_activate_helper(self, symbol: str, positions: Dict, current_price: float) -> bool:
        """Determine if helper orders should be activated"""
        helper_config = self.config['vortex_dca']

        # Check if helper is enabled
        if not helper_config.get('helper_enabled', False):
            self.logger.debug(f"[{symbol}] Helper disabled in config")
            return False

        # Determine current larger position
        long_qty = positions['long']['qty']
        short_qty = positions['short']['qty']

        if long_qty == 0 and short_qty == 0:
            return False  # No positions

        current_larger_side = "long" if long_qty > short_qty else "short"

        # Check if helper is already active
        if self.helper_state[symbol]['active']:
            active_side = self.helper_state[symbol].get('side')

            # FORCE CANCEL if side changed (even if duration hasn't expired)
            if active_side and active_side != current_larger_side:
                self.logger.warning(f"[{symbol}] Larger position changed from {active_side} to {current_larger_side} while helper active! Force cancelling helper orders...")

                # Cancel ONLY the tracked helper orders
                helper_order_ids = self.helper_state[symbol]['orders']
                self.logger.warning(f"[{symbol}] Cancelling {len(helper_order_ids)} tracked {active_side} helper orders")

                cancelled_count = 0
                for order_id in helper_order_ids:
                    try:
                        self.exchange.cancel_order(order_id, symbol)
                        cancelled_count += 1
                        self.logger.info(f"[{symbol}] ✅ Cancelled {active_side} helper order {order_id}")
                    except Exception as e:
                        self.logger.warning(f"[{symbol}] Failed to cancel helper {order_id}: {e}")

                self.logger.info(f"[{symbol}] Force-cancelled {cancelled_count}/{len(helper_order_ids)} {active_side} helper orders")

                # Reset helper state
                self.helper_state[symbol] = {'active': False, 'orders': [], 'placed_at': 0, 'side': None}
                self.logger.info(f"[{symbol}] Helper reset due to position side change")
                # Continue to check if we should activate for new side
            else:
                self.logger.debug(f"[{symbol}] Helper already active for {active_side}")
                return False

        # Check cooldown period
        current_time = time.time()
        last_activated = self.helper_last_activated[symbol]
        cooldown = helper_config.get('helper_cooldown', 300)  # Default 5 minutes
        time_since_last = current_time - last_activated

        if current_time - last_activated < cooldown:
            self.logger.debug(f"[{symbol}] Helper in cooldown ({time_since_last:.0f}s / {cooldown}s)")
            return False

        # Check if we have a position that meets threshold
        threshold = helper_config.get('helper_activation_threshold_qty', 0.0)

        # Activate if either position exceeds threshold
        if long_qty > threshold or short_qty > threshold:
            self.logger.info(f"[{symbol}] ✅ Helper activation conditions met: long={long_qty}, short={short_qty}, threshold={threshold}")
            return True

        # Don't spam logs - only log occasionally
        self.logger.debug(f"[{symbol}] Helper not activated: long={long_qty}, short={short_qty}, threshold={threshold}")
        return False

    def _place_helper_orders(self, symbol: str, positions: Dict, current_price: float) -> None:
        """Place helper wall orders for the larger position"""
        try:
            helper_config = self.config['vortex_dca']

            # Get orderbook for best bid/ask
            try:
                orderbook = self.exchange.exchange.fetch_order_book(symbol)
                best_bid_price = float(orderbook['bids'][0][0]) if orderbook['bids'] else current_price
                best_ask_price = float(orderbook['asks'][0][0]) if orderbook['asks'] else current_price
            except Exception as e:
                self.logger.warning(f"[{symbol}] Could not fetch orderbook for helper, using current price: {e}")
                best_bid_price = current_price
                best_ask_price = current_price

            long_qty = positions['long']['qty']
            short_qty = positions['short']['qty']

            # Determine which position is larger
            if long_qty == 0 and short_qty == 0:
                self.logger.warning(f"[{symbol}] No positions found, skipping helper orders")
                return

            larger_position = "long" if long_qty > short_qty else "short"

            self.logger.info(f"[{symbol}] 📊 HELPER PLACEMENT TRIGGERED")
            self.logger.info(f"[{symbol}] Current positions - Long: {long_qty}, Short: {short_qty}")
            self.logger.info(f"[{symbol}] Larger position: {larger_position}")

            # Check if we have active helper orders for the OPPOSITE side
            if self.helper_state[symbol]['active']:
                active_side = self.helper_state[symbol].get('side')
                self.logger.warning(f"[{symbol}] Helper already active for {active_side} side")
                if active_side and active_side != larger_position:
                    self.logger.warning(f"[{symbol}] 🚨 POSITION SIDE MISMATCH in _place_helper_orders()!")

                    # Cancel ONLY the tracked helper orders
                    helper_order_ids = self.helper_state[symbol]['orders']
                    self.logger.warning(f"[{symbol}] Cancelling {len(helper_order_ids)} old {active_side} helper orders")

                    cancelled_count = 0
                    for order_id in helper_order_ids:
                        try:
                            self.exchange.cancel_order(order_id, symbol)
                            cancelled_count += 1
                            self.logger.info(f"[{symbol}] ✅ Cancelled old {active_side} helper order {order_id}")
                        except Exception as e:
                            self.logger.warning(f"[{symbol}] Failed to cancel helper {order_id}: {e}")

                    self.logger.info(f"[{symbol}] Cancelled {cancelled_count}/{len(helper_order_ids)} old {active_side} helper orders")

                    # Reset helper state
                    self.helper_state[symbol] = {'active': False, 'orders': [], 'placed_at': 0, 'side': None}

            # Get helper configuration
            wall_size = helper_config.get('helper_wall_size', 5)
            helper_multiplier = helper_config.get('helper_multiplier', 1.5)
            price_gap_pct = helper_config.get('helper_price_gap_pct', 0.006)  # 0.6%

            # Calculate base amount for helper orders
            if larger_position == "long":
                base_amount = long_qty * helper_multiplier
                reference_price = best_bid_price  # Use bid for buy orders below
            else:
                base_amount = short_qty * helper_multiplier
                reference_price = best_ask_price  # Use ask for sell orders above

            helper_orders = []

            self.logger.info(f"[{symbol}] 🏗️ Placing {wall_size} {larger_position} helper orders (size: {base_amount:.4f} each)")

            # Place wall of orders
            for i in range(wall_size):
                try:
                    # Calculate gap with increasing distance
                    gap = reference_price * price_gap_pct * (1 + i * 0.5)  # Each level 50% farther

                    if larger_position == "long":
                        # Place buy orders below best bid (support)
                        helper_price = best_bid_price - gap
                        order_side = 'buy'
                        position_side = 'long'
                    else:
                        # Place sell orders above best ask (resistance)
                        helper_price = best_ask_price + gap
                        order_side = 'sell'
                        position_side = 'short'

                    # Place order using exchange-agnostic method (NOT reduce-only)
                    result = self.exchange._place_order_with_position_side(
                        symbol=symbol,
                        side=order_side,
                        amount=base_amount,
                        price=helper_price,
                        order_type='limit',
                        reduce_only=False,  # Helper orders add to position (not reduce-only)
                        position_side=position_side,
                        post_only=True  # Try to use post-only if supported
                    )

                    if result:
                        helper_orders.append(result['id'])
                        self.logger.info(f"[{symbol}] Helper order {i+1}/{wall_size}: {order_side} {base_amount:.4f} @ {helper_price:.4f}")
                    else:
                        self.logger.warning(f"[{symbol}] Failed to place helper order {i+1}")

                    time.sleep(0.05)  # Small delay between orders

                except Exception as e:
                    self.logger.error(f"[{symbol}] Error placing helper order {i+1}: {e}")

            # Update helper state
            if helper_orders:
                self.helper_state[symbol] = {
                    'active': True,
                    'orders': helper_orders,
                    'placed_at': time.time(),
                    'side': larger_position
                }
                self.helper_last_activated[symbol] = time.time()
                self.logger.info(f"[{symbol}] ✅ HELPER ACTIVATED for {larger_position.upper()} side")
                self.logger.info(f"[{symbol}] Placed {len(helper_orders)} helper orders: {helper_orders}")
                self.logger.info(f"[{symbol}] Helper state: {self.helper_state[symbol]}")
            else:
                self.logger.warning(f"[{symbol}] No helper orders were successfully placed")

        except Exception as e:
            self.logger.error(f"[{symbol}] Error in helper order placement: {e}")

    def _manage_helper_orders(self, symbol: str) -> None:
        """Manage active helper orders - cancel when duration expires OR position side changed"""
        if not self.helper_state[symbol]['active']:
            return

        helper_config = self.config['vortex_dca']
        current_time = time.time()
        placed_at = self.helper_state[symbol]['placed_at']
        duration = helper_config.get('helper_duration', 60)  # Default 60 seconds

        # Get current positions to check if side changed
        try:
            positions = self.exchange.get_positions_cached(symbol)
            if positions:
                long_qty = positions['long']['qty']
                short_qty = positions['short']['qty']
                current_larger_side = "long" if long_qty > short_qty else "short"
                active_side = self.helper_state[symbol].get('side')

                # FORCE CANCEL if position side changed
                if active_side and active_side != current_larger_side and (long_qty > 0 or short_qty > 0):
                    self.logger.warning(f"[{symbol}] 🚨 HELPER SIDE MISMATCH DETECTED in _manage_helper_orders()!")
                    self.logger.warning(f"[{symbol}] Active helper: {active_side}, Current larger: {current_larger_side}")
                    self.logger.warning(f"[{symbol}] Positions - Long: {long_qty}, Short: {short_qty}")

                    # Cancel ONLY the tracked helper orders
                    helper_order_ids = self.helper_state[symbol]['orders']
                    self.logger.warning(f"[{symbol}] Force cancelling {len(helper_order_ids)} {active_side} helper orders")

                    cancelled_count = 0
                    for order_id in helper_order_ids:
                        try:
                            self.exchange.cancel_order(order_id, symbol)
                            cancelled_count += 1
                            self.logger.info(f"[{symbol}] ✅ Force cancelled {active_side} helper order {order_id}")
                        except Exception as e:
                            self.logger.warning(f"[{symbol}] Failed to cancel helper {order_id}: {e}")

                    self.logger.info(f"[{symbol}] Helper force-cancelled due to side change: {cancelled_count}/{len(helper_order_ids)} orders")

                    self.helper_state[symbol] = {'active': False, 'orders': [], 'placed_at': 0, 'side': None}
                    return
        except Exception as e:
            self.logger.debug(f"[{symbol}] Could not check positions in _manage_helper_orders: {e}")

        # Check if duration has expired
        if current_time - placed_at >= duration:
            active_side = self.helper_state[symbol].get('side', 'unknown')
            helper_order_ids = self.helper_state[symbol]['orders']
            self.logger.info(f"[{symbol}] Helper duration expired ({duration}s), cancelling {len(helper_order_ids)} {active_side} helper orders")

            # Cancel all tracked helper orders
            cancelled_count = 0
            for order_id in helper_order_ids:
                try:
                    self.exchange.cancel_order(order_id, symbol)
                    cancelled_count += 1
                    self.logger.debug(f"[{symbol}] Cancelled helper order {order_id}")
                except Exception as e:
                    self.logger.warning(f"[{symbol}] Failed to cancel helper order {order_id}: {e}")

            self.logger.info(f"[{symbol}] Helper deactivated: {cancelled_count}/{len(helper_order_ids)} helper orders cancelled")

            # Deactivate helper
            self.helper_state[symbol] = {
                'active': False,
                'orders': [],
                'placed_at': 0,
                'side': None
            }

    def get_status(self) -> Dict:
        """Get strategy status"""
        status = {
            'running': self.running,
            'symbols': {},
            'total_symbols': len(self.calculators)
        }
        
        for symbol, calculator in self.calculators.items():
            try:
                positions = self.exchange.get_positions_cached(symbol)
                current_price = self.exchange.get_current_price_cached(symbol)
                
                status['symbols'][symbol] = {
                    'current_price': current_price,
                    'positions': positions,
                    'grid_info': {
                        'long': calculator.get_grid_info('long'),
                        'short': calculator.get_grid_info('short')
                    },
                    'last_refresh_price': self.last_refresh_prices.get(symbol)
                }
            except Exception as e:
                status['symbols'][symbol] = {'error': str(e)}
                
        return status
    
    def _handle_removed_symbols(self) -> None:
        """Handle positions for symbols that are no longer in the active trading list"""
        try:
            self.logger.info("=== CHECKING FOR REMOVED SYMBOL POSITIONS ===")
            
            # Get all open orders to find what symbols have activity
            all_orders = self.exchange.get_open_orders()  # Get all orders without symbol filter
            symbols_with_orders = set()
            
            # Extract unique symbols from all orders and normalize them
            for order in all_orders:
                if 'symbol' in order:
                    # Convert CCXT format (BIO/USDT:USDT) to exchange format (BIOUSDT)
                    symbol = order['symbol']
                    normalized_symbol = symbol.replace('/', '').replace(':USDT', '')
                    symbols_with_orders.add(normalized_symbol)
            
            # Convert active symbols to same format for comparison
            normalized_active = set()
            for symbol in self.active_symbols:
                normalized_symbol = symbol.replace('/', '').replace(':USDT', '')
                normalized_active.add(normalized_symbol)
            
            # Check positions for symbols that have orders but aren't in active list
            removed_symbols = symbols_with_orders - normalized_active
            
            if removed_symbols:
                self.logger.warning(f"Found {len(removed_symbols)} symbols with activity not in active list: {list(removed_symbols)}")
            
            for symbol in removed_symbols:
                try:
                    # Get positions for this symbol
                    positions = self.exchange.get_positions_cached(symbol)
                    current_price = self.exchange.get_current_price_cached(symbol)
                    
                    if not current_price:
                        continue
                        
                    # Check if we have any positions
                    has_position = positions['long']['qty'] > 0 or positions['short']['qty'] > 0
                    
                    if has_position:
                        self.logger.critical(f"[{symbol}] REMOVED SYMBOL has position - setting up exit TP")
                        
                        # Cancel existing orders for this symbol (except TPs)
                        try:
                            open_orders = self.exchange.get_open_orders(symbol)
                            for order in open_orders:
                                if not order.get('reduce_only', False):  # Don't cancel existing TPs
                                    self.exchange.cancel_order(order['id'], symbol)
                                    self.logger.info(f"[{symbol}] Cancelled grid order {order['id']}")
                        except Exception as e:
                            self.logger.warning(f"[{symbol}] Error cancelling orders: {e}")
                        
                        # Place TP orders for positions
                        if positions['long']['qty'] > 0:
                            self._place_exit_take_profit(symbol, positions['long'], 'long', current_price)
                        if positions['short']['qty'] > 0:
                            self._place_exit_take_profit(symbol, positions['short'], 'short', current_price)
                    
                    else:
                        # Has orders but no position - just cancel the grid orders
                        self.logger.info(f"[{symbol}] REMOVED SYMBOL has orders but no position - cancelling grid orders")
                        try:
                            open_orders = self.exchange.get_open_orders(symbol)
                            for order in open_orders:
                                if not order.get('reduce_only', False):
                                    self.exchange.cancel_order(order['id'], symbol)
                                    self.logger.info(f"[{symbol}] Cancelled grid order {order['id']}")
                        except Exception as e:
                            self.logger.warning(f"[{symbol}] Error cancelling orders: {e}")
                
                except Exception as e:
                    self.logger.error(f"[{symbol}] Error checking removed symbol: {e}")
                    continue
            
            if not removed_symbols:
                self.logger.info("No removed symbols with positions found")
                
        except Exception as e:
            self.logger.error(f"Error handling removed symbols: {e}")
    
    def _place_exit_take_profit(self, symbol: str, position: Dict, side: str, current_price: float) -> None:
        """Place take profit order for a position in a removed symbol"""
        try:
            qty = position['qty']
            entry_price = position['entry_price']
            
            # Use minimum TP percentage from config
            min_tp_pct = self.config['vortex_dca']['minimum_tp']
            
            # Calculate TP price
            if side == 'long':
                tp_price = entry_price * (1 + min_tp_pct)
                tp_side = 'sell'
            else:  # short
                tp_price = entry_price * (1 - min_tp_pct)
                tp_side = 'buy'
            
            self.logger.critical(f"[{symbol}] REMOVED SYMBOL EXIT: Placing TP {tp_side} {qty} @ {tp_price} (entry: {entry_price})")
            
            # Place the exit TP order
            result = self.exchange.place_take_profit_order(
                symbol=symbol,
                side=tp_side,
                amount=qty,
                price=tp_price,
                position_side=side
            )
            
            if result:
                self.logger.critical(f"[{symbol}] ✅ EXIT TP PLACED: {result['id']} - {tp_side} {qty} @ {tp_price}")
            else:
                self.logger.critical(f"[{symbol}] ❌ FAILED to place exit TP order")

        except Exception as e:
            self.logger.error(f"[{symbol}] Error placing exit TP for removed symbol: {e}")

    def _check_auto_hedge(self, symbol: str, positions: Dict, current_price: float) -> None:
        """Check if auto-hedge should trigger based on NET position only (prevents hedge-against-hedge spiral)"""
        if not self.autohedge_enabled:
            return

        try:
            long_qty = positions['long']['qty']
            short_qty = positions['short']['qty']
            long_entry = positions['long']['entry_price']
            short_entry = positions['short']['entry_price']
            long_liq = positions['long'].get('liq_price', 0)
            short_liq = positions['short'].get('liq_price', 0)

            # Calculate NET position
            net_qty = long_qty - short_qty

            # Skip if no net position (perfectly balanced)
            if abs(net_qty) < 1:
                return

            # Determine which side is the NET position
            if net_qty > 0:
                # Net LONG
                net_side = 'long'
                net_entry = long_entry if long_entry > 0 else current_price
                liq_price = long_liq

                # Calculate drawdown for NET LONG
                drawdown_pct = (net_entry - current_price) / net_entry if current_price < net_entry and net_entry > 0 else 0

                # Calculate liquidation distance
                liq_distance_pct = (current_price - liq_price) / current_price if liq_price > 0 else 999

            else:
                # Net SHORT
                net_side = 'short'
                net_entry = short_entry if short_entry > 0 else current_price
                liq_price = short_liq

                # Calculate drawdown for NET SHORT
                drawdown_pct = (current_price - net_entry) / net_entry if current_price > net_entry and net_entry > 0 else 0

                # Calculate liquidation distance
                liq_distance_pct = (liq_price - current_price) / current_price if liq_price > 0 else 999

            # Check if hedge should trigger
            should_hedge_drawdown = drawdown_pct >= self.autohedge_on_drawdown_pct
            should_hedge_liq = liq_distance_pct > 0 and liq_distance_pct <= self.autohedge_on_liquidation_distance_pct

            if not (should_hedge_drawdown or should_hedge_liq):
                return

            # Determine trigger reason
            trigger = "DRAWDOWN" if should_hedge_drawdown else "LIQUIDATION"
            self.logger.warning(f"[{symbol}] NET {net_side.upper()} 🛡️ AUTO-HEDGE TRIGGER: {trigger}")
            self.logger.warning(f"   Net Position: {abs(net_qty):.0f} {net_side.upper()} @ ${net_entry:.5f}")
            self.logger.warning(f"   Current: ${current_price:.5f} | Drawdown: {drawdown_pct*100:.2f}%")
            if liq_distance_pct < 999:
                self.logger.warning(f"   Liquidation: ${liq_price:.5f} | Distance: {liq_distance_pct*100:.1f}%")

            # Place hedge on opposite side (hedge the NET position)
            self._place_hedge_order(symbol, net_side, abs(net_qty), current_price, positions)

        except Exception as e:
            self.logger.error(f"[{symbol}] ❌ Error checking auto-hedge: {e}", exc_info=True)

    def _place_hedge_order(self, symbol: str, position_side: str, position_qty: float, current_price: float, positions: Dict) -> None:
        """Place hedge order on opposite side"""
        try:
            # Track ORIGINAL position size to prevent cascade
            last_hedge = self.last_hedge_info.get(symbol, {}).get(position_side)

            # Determine original qty: use last hedge's original qty if exists, otherwise current qty
            if last_hedge and 'original_qty' in last_hedge:
                original_qty = last_hedge['original_qty']
                # Reset if position changed direction or grew significantly
                if abs(position_qty - last_hedge['qty']) / last_hedge['qty'] > 0.5:
                    original_qty = position_qty
                    self.logger.info(f"[{symbol}] {position_side.upper()} 🆕 Position changed significantly - resetting original qty to {original_qty:.4f}")
            else:
                original_qty = position_qty
                self.logger.info(f"[{symbol}] {position_side.upper()} 🆕 Starting new hedge sequence - original qty: {original_qty:.4f}")

            # Calculate how much we've ALREADY hedged on the opposite side
            opposite_side = 'short' if position_side == 'long' else 'long'
            opposite_qty = positions[opposite_side]['qty']

            # Calculate hedge ratio achieved so far
            hedge_ratio_achieved = opposite_qty / original_qty if original_qty > 0 else 0

            self.logger.info(f"[{symbol}] {position_side.upper()} 📊 Hedge ratio: {hedge_ratio_achieved*100:.1f}% (target: {self.autohedge_ratio*100:.0f}%)")

            # Check if we've already hedged enough
            if hedge_ratio_achieved >= self.autohedge_ratio * 0.95:  # Within 5% of target
                self.logger.info(f"[{symbol}] {position_side.upper()} ⏸️ Already hedged {hedge_ratio_achieved*100:.0f}% >= target, RESPECTING HEDGE RATIO")
                return

            # Calculate remaining qty to hedge
            target_hedge_qty = original_qty * self.autohedge_ratio
            remaining_to_hedge = target_hedge_qty - opposite_qty

            if remaining_to_hedge <= 0:
                self.logger.info(f"[{symbol}] {position_side.upper()} ⏸️ Hedge complete, no more hedging needed")
                return

            # Determine hedge order side and price
            if position_side == 'long':
                hedge_side = 'sell'  # Sell to hedge long
                hedge_price = current_price * 0.999  # Slightly below market for quick fill
            else:  # short position
                hedge_side = 'buy'  # Buy to hedge short
                hedge_price = current_price * 1.001  # Slightly above market for quick fill

            self.logger.warning(f"[{symbol}] {position_side.upper()} 🛡️ PLACING HEDGE: {hedge_side} {remaining_to_hedge:.4f} @ ${hedge_price:.5f}")

            # Place hedge order as MARKET for immediate execution
            result = self.exchange.place_order(
                symbol=symbol,
                side=hedge_side,
                amount=remaining_to_hedge,
                price=None,  # MARKET order
                order_type='market',
                reduce_only=False
            )

            if result:
                self.logger.warning(f"[{symbol}] {position_side.upper()} ✅ HEDGE PLACED: {result.get('id', 'N/A')}")

                # Update hedge tracking
                if symbol not in self.last_hedge_info:
                    self.last_hedge_info[symbol] = {}
                self.last_hedge_info[symbol][position_side] = {
                    'price': current_price,
                    'qty': position_qty,
                    'original_qty': original_qty,
                    'timestamp': time.time()
                }

                # Schedule TP placement for hedge (after short delay to let hedge fill)
                time.sleep(1)
                self._place_hedge_tp(symbol, opposite_side, remaining_to_hedge, current_price)
            else:
                self.logger.error(f"[{symbol}] {position_side.upper()} ❌ Hedge order returned empty result")

        except Exception as e:
            self.logger.error(f"[{symbol}] ❌ Error placing hedge order: {e}", exc_info=True)

    def _place_hedge_tp(self, symbol: str, hedge_side: str, hedge_qty: float, hedge_price: float) -> None:
        """Place take profit order on hedge position"""
        try:
            # Calculate TP price based on hedge TP target
            if hedge_side == 'long':
                tp_price = hedge_price * (1 + self.autohedge_tp_target)
                tp_order_side = 'sell'
            else:  # short hedge
                tp_price = hedge_price * (1 - self.autohedge_tp_target)
                tp_order_side = 'buy'

            self.logger.info(f"[{symbol}] {hedge_side.upper()} HEDGE TP: Placing {tp_order_side} {hedge_qty:.4f} @ ${tp_price:.5f}")

            result = self.exchange.place_take_profit_order(
                symbol=symbol,
                side=tp_order_side,
                amount=hedge_qty,
                price=tp_price,
                position_side=hedge_side
            )

            if result:
                self.logger.info(f"[{symbol}] {hedge_side.upper()} HEDGE ✅ TP PLACED: {result.get('id', 'N/A')}")

                # Track hedge TP order with entry details for trailing stop
                if symbol not in self.hedge_tp_orders:
                    self.hedge_tp_orders[symbol] = {}
                self.hedge_tp_orders[symbol][hedge_side] = {
                    'order_id': result.get('id'),
                    'placed_at': time.time(),
                    'hedge_entry_price': hedge_price,
                    'hedge_qty': hedge_qty,
                    'tp_price': tp_price,
                    'trailing_active': False  # Will activate when profit target reached
                }

                # Initialize best price tracking (high water mark for trailing)
                if symbol not in self.hedge_best_prices:
                    self.hedge_best_prices[symbol] = {}
                self.hedge_best_prices[symbol][hedge_side] = hedge_price

            else:
                self.logger.warning(f"[{symbol}] {hedge_side.upper()} HEDGE ⚠️ TP placement returned empty result")

        except Exception as e:
            self.logger.error(f"[{symbol}] ❌ Error placing hedge TP: {e}", exc_info=True)

    def _monitor_hedge_trailing_stop(self, symbol: str, positions: Dict, current_price: float) -> None:
        """Monitor hedge positions and update trailing stops when profitable"""
        if not self.autohedge_trailing_enabled:
            return

        try:
            # Check if we have any tracked hedges for this symbol
            if symbol not in self.hedge_tp_orders:
                return

            long_qty = positions['long']['qty']
            short_qty = positions['short']['qty']

            # Check each tracked hedge
            for hedge_side in ['long', 'short']:
                if hedge_side not in self.hedge_tp_orders[symbol]:
                    continue

                hedge_info = self.hedge_tp_orders[symbol][hedge_side]

                # Verify hedge position still exists
                current_qty = long_qty if hedge_side == 'long' else short_qty
                if current_qty <= 0:
                    # Hedge position closed - clean up tracking
                    self.logger.info(f"[{symbol}] {hedge_side.upper()} HEDGE: Position closed, cleaning up tracking")
                    del self.hedge_tp_orders[symbol][hedge_side]
                    if hedge_side in self.hedge_best_prices.get(symbol, {}):
                        del self.hedge_best_prices[symbol][hedge_side]
                    continue

                hedge_entry = hedge_info['hedge_entry_price']

                # Calculate current P&L percentage
                if hedge_side == 'long':
                    pnl_pct = (current_price - hedge_entry) / hedge_entry
                else:  # short
                    pnl_pct = (hedge_entry - current_price) / hedge_entry

                # Check if we should activate trailing stop
                if not hedge_info['trailing_active']:
                    if pnl_pct >= self.autohedge_tp_target:
                        # Profit target reached - activate trailing stop
                        self.logger.info(f"[{symbol}] {hedge_side.upper()} HEDGE 🎯 PROFIT TARGET REACHED: {pnl_pct*100:.2f}% - Activating trailing stop")
                        hedge_info['trailing_active'] = True
                        self.hedge_best_prices[symbol][hedge_side] = current_price

                # If trailing is active, update best price and check if we should close
                if hedge_info['trailing_active']:
                    best_price = self.hedge_best_prices[symbol][hedge_side]

                    # Update best price if current is better
                    if hedge_side == 'long':
                        if current_price > best_price:
                            self.logger.info(f"[{symbol}] {hedge_side.upper()} HEDGE: Updating best price ${best_price:.5f} → ${current_price:.5f}")
                            self.hedge_best_prices[symbol][hedge_side] = current_price
                            best_price = current_price

                        # Check if price has retraced by trailing distance
                        retrace_pct = (best_price - current_price) / best_price

                    else:  # short
                        if current_price < best_price:
                            self.logger.info(f"[{symbol}] {hedge_side.upper()} HEDGE: Updating best price ${best_price:.5f} → ${current_price:.5f}")
                            self.hedge_best_prices[symbol][hedge_side] = current_price
                            best_price = current_price

                        # Check if price has retraced by trailing distance
                        retrace_pct = (current_price - best_price) / best_price

                    # If retraced beyond trailing distance, close hedge at profit
                    if retrace_pct >= self.autohedge_trailing_distance:
                        best_pnl_pct = (best_price - hedge_entry) / hedge_entry if hedge_side == 'long' else (hedge_entry - best_price) / hedge_entry
                        self.logger.warning(f"[{symbol}] {hedge_side.upper()} HEDGE 💰 TRAILING STOP TRIGGERED: Retraced {retrace_pct*100:.2f}% from best (${best_price:.5f} → ${current_price:.5f}), closing at {pnl_pct*100:.2f}% profit (best was {best_pnl_pct*100:.2f}%)")

                        # Cancel existing TP order
                        try:
                            if 'order_id' in hedge_info:
                                self.exchange.cancel_order(symbol, hedge_info['order_id'])
                                self.logger.info(f"[{symbol}] {hedge_side.upper()} HEDGE: Cancelled TP order {hedge_info['order_id']}")
                        except Exception as e:
                            self.logger.warning(f"[{symbol}] {hedge_side.upper()} HEDGE: Could not cancel TP order: {e}")

                        # Place market order to close hedge at current profit
                        close_side = 'sell' if hedge_side == 'long' else 'buy'
                        self.logger.info(f"[{symbol}] {hedge_side.upper()} HEDGE: Placing market {close_side} {current_qty:.4f} @ ${current_price:.5f}")

                        result = self.exchange.place_order(
                            symbol=symbol,
                            side=close_side,
                            order_type='market',
                            amount=current_qty,
                            reduce_only=True,
                            position_side=hedge_side
                        )

                        if result:
                            self.logger.info(f"[{symbol}] {hedge_side.upper()} HEDGE ✅ CLOSED WITH TRAILING PROFIT: {result.get('id', 'N/A')} - {pnl_pct*100:.2f}%")

                            # Clean up tracking
                            del self.hedge_tp_orders[symbol][hedge_side]
                            if hedge_side in self.hedge_best_prices.get(symbol, {}):
                                del self.hedge_best_prices[symbol][hedge_side]
                        else:
                            self.logger.error(f"[{symbol}] {hedge_side.upper()} HEDGE ❌ Failed to place trailing stop close order")

        except Exception as e:
            self.logger.error(f"[{symbol}] ❌ Error monitoring hedge trailing stop: {e}", exc_info=True)
