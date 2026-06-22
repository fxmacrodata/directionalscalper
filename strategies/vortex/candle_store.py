"""
Candle fetching and caching system for historical data analysis
"""

import logging
import time
from typing import List, Dict, Optional
from collections import defaultdict


class CandleStore:
    """
    Manages fetching and caching of historical candle data
    """

    def __init__(self, exchange, logger=None):
        self.exchange = exchange
        self.logger = logger or logging.getLogger(__name__)

        # Cache structure: {symbol: {timeframe: {'candles': [...], 'fetched_at': timestamp}}}
        self.cache = defaultdict(lambda: defaultdict(dict))
        self.cache_duration = 300  # Cache candles for 5 minutes

    def get_candles(
        self,
        symbol: str,
        timeframe: str = '1m',
        period_str: str = '1H',
        limit: Optional[int] = None
    ) -> List[dict]:
        """
        Get historical candles with caching

        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT')
            timeframe: Candle timeframe ('1m', '5m', '15m', '1h', etc.)
            period_str: Period to fetch (e.g., '1H', '4H', '1D', '1W', '1M')
            limit: Max number of candles to return (calculated from period if not provided)

        Returns:
            List of candle dictionaries [{'open': x, 'high': y, 'low': z, 'close': w, 'volume': v, 'timestamp': t}, ...]
        """
        cache_key = f"{timeframe}_{period_str}"

        # Check cache
        if symbol in self.cache and cache_key in self.cache[symbol]:
            cached_data = self.cache[symbol][cache_key]
            if time.time() - cached_data.get('fetched_at', 0) < self.cache_duration:
                self.logger.debug(f"[{symbol}] Using cached candles for {timeframe}/{period_str}")
                return cached_data['candles']

        # Calculate limit from period
        if limit is None:
            limit = self._period_to_candle_count(period_str, timeframe)

        self.logger.info(f"[{symbol}] Fetching {limit} {timeframe} candles (period: {period_str})")

        try:
            # Convert symbol to exchange format if needed (for BloFin, HTX, etc.)
            fetch_symbol = symbol
            if hasattr(self.exchange, 'convert_symbol_format'):
                fetch_symbol = self.exchange.convert_symbol_format(symbol)
                self.logger.debug(f"[{symbol}] Converted to {fetch_symbol} for candle fetch")

            # Fetch candles from exchange
            # Try direct fetch_ohlcv first (for non-CCXT exchanges like Aster)
            if hasattr(self.exchange, 'fetch_ohlcv'):
                ohlcv = self.exchange.fetch_ohlcv(fetch_symbol, timeframe, limit=limit)
            elif hasattr(self.exchange, 'exchange') and self.exchange.exchange:
                ohlcv = self.exchange.exchange.fetch_ohlcv(fetch_symbol, timeframe, limit=limit)
            else:
                raise AttributeError("Exchange does not support fetch_ohlcv")

            # Convert to our format
            candles = []
            for candle in ohlcv:
                candles.append({
                    'timestamp': candle[0],
                    'open': candle[1],
                    'high': candle[2],
                    'low': candle[3],
                    'close': candle[4],
                    'volume': candle[5]
                })

            # Cache the result
            self.cache[symbol][cache_key] = {
                'candles': candles,
                'fetched_at': time.time()
            }

            self.logger.info(f"[{symbol}] Fetched {len(candles)} candles successfully")
            return candles

        except Exception as e:
            self.logger.error(f"[{symbol}] Error fetching candles: {e}")
            return []

    def _period_to_candle_count(self, period_str: str, timeframe: str) -> int:
        """
        Convert period string to number of candles needed

        Args:
            period_str: Period like '1H', '4H', '1D', '1W', '1M'
            timeframe: Timeframe like '1m', '5m', '1h'

        Returns:
            Number of candles to fetch
        """
        # Parse period
        period_value = int(period_str[:-1])
        period_unit = period_str[-1].upper()

        # Convert to minutes
        period_minutes = {
            'M': period_value,  # Minutes
            'H': period_value * 60,  # Hours
            'D': period_value * 1440,  # Days
            'W': period_value * 10080,  # Weeks
        }.get(period_unit, period_value * 60)  # Default to hours

        # Parse timeframe
        if timeframe.endswith('m'):
            tf_minutes = int(timeframe[:-1])
        elif timeframe.endswith('h'):
            tf_minutes = int(timeframe[:-1]) * 60
        elif timeframe.endswith('d'):
            tf_minutes = int(timeframe[:-1]) * 1440
        else:
            tf_minutes = 1  # Default to 1 minute

        # Calculate candle count
        candle_count = period_minutes // tf_minutes

        # Cap at reasonable limits
        return min(max(candle_count, 10), 1000)  # Between 10 and 1000 candles

    def clear_cache(self, symbol: Optional[str] = None):
        """Clear cache for a symbol or all symbols"""
        if symbol:
            if symbol in self.cache:
                del self.cache[symbol]
                self.logger.info(f"[{symbol}] Cleared candle cache")
        else:
            self.cache.clear()
            self.logger.info("Cleared all candle caches")
