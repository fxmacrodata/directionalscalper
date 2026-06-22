"""
Base Exchange Interface
Minimal interface for exchange operations
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Tuple, Optional
import logging


class BaseExchange(ABC):
    """Abstract base class for exchange implementations"""
    
    def __init__(self, config: Dict, logger=None):
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self.exchange = None
        
    @abstractmethod
    def connect(self) -> bool:
        """Connect to exchange"""
        pass
        
    @abstractmethod
    def get_balance(self) -> float:
        """Get total wallet balance in USDT"""
        pass
        
    @abstractmethod
    def get_current_price(self, symbol: str) -> float:
        """Get current market price for symbol"""
        pass
        
    @abstractmethod
    def get_positions(self) -> List[Dict]:
        """Get all open positions"""
        pass
        
    @abstractmethod
    def get_open_orders(self, symbol: str = None) -> List[Dict]:
        """Get open orders for symbol or all symbols"""
        pass
        
    @abstractmethod
    def place_order(
        self,
        symbol: str,
        side: str,
        amount: float,
        price: float,
        order_type: str = "limit",
        reduce_only: bool = False
    ) -> Dict:
        """Place an order"""
        pass
        
    @abstractmethod
    def cancel_order(self, order_id: str, symbol: str) -> bool:
        """Cancel an order"""
        pass
        
    @abstractmethod
    def cancel_all_orders(self, symbol: str) -> bool:
        """Cancel all orders for a symbol"""
        pass
        
    def get_symbol_info(self, symbol: str) -> Dict:
        """Get symbol trading information"""
        try:
            if self.exchange:
                if not hasattr(self.exchange, 'markets') or not self.exchange.markets:
                    self.logger.info(f"Loading markets for {symbol}...")
                    self.exchange.load_markets()
                
                # Use CCXT's built-in market() method for symbol conversion
                return self.exchange.market(symbol)
            return {}
        except Exception as e:
            self.logger.error(f"Error getting symbol info for {symbol}: {e}")
            return {}
            
    def get_precision_and_limits(self, symbol: str):
        """Get precision and limits for symbol - matches original implementation"""
        try:
            # Use CCXT market() method like original
            market = self.exchange.market(symbol)
            precision_amount = market['precision']['amount']
            precision_price = market['precision']['price']
            min_amount = market['limits']['amount']['min']
            return precision_amount, precision_price, min_amount
        except Exception as e:
            self.logger.error(f"Error getting precision for {symbol}: {e}")
            return None, None, None

    def get_exchange_rules(self, symbol: str) -> dict:
        """Get exchange constraints for order validation.

        Returns:
            dict with min_order_value, min_qty, qty_step, tick_size
        """
        try:
            market = self.exchange.market(symbol)
            # CCXT stores None for unknown values, so use `or` to fallback
            return {
                'min_order_value': market.get('limits', {}).get('cost', {}).get('min') or 5.0,
                'min_qty': market.get('limits', {}).get('amount', {}).get('min') or 1.0,
                'qty_step': market.get('precision', {}).get('amount') or 1.0,
                'tick_size': market.get('precision', {}).get('price') or 0.0001,
            }
        except Exception as e:
            self.logger.error(f"get_exchange_rules({symbol}): {e}")
            return {'min_order_value': 5.0, 'min_qty': 1.0, 'qty_step': 1.0, 'tick_size': 0.0001}