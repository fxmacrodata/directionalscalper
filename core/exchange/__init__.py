from .base import BaseExchange
from .bybit import BybitExchange
from .blofin import BloFinExchange
from .aster import AsterExchange

__all__ = ['BaseExchange', 'BybitExchange', 'BloFinExchange', 'AsterExchange']

# AsterExchangeV3 needs eth_account for EIP-712 signing. Import lazily so a
# missing dependency does not crash the whole exchange package.
try:
    from .aster_v3 import AsterExchangeV3  # noqa: F401
    __all__.append('AsterExchangeV3')
except ImportError:
    pass
