#!/usr/bin/env python3
"""
Vortex DCA Bot - Main Entry Point
Uses raw dict config.
"""

import argparse
import json
import logging
import logging.handlers
import signal
import sys
import time
from pathlib import Path

from core.exchange.bybit import BybitExchange
from core.exchange.blofin import BloFinExchange
from core.exchange.aster import AsterExchange
from core.exchange.aster_v3 import AsterExchangeV3
from strategies.vortex.main import VortexDCAStrategy


class TradingBot:
    """Vortex DCA bot orchestrator"""

    def __init__(self, config_path: str):
        with open(config_path) as f:
            self.config = json.load(f)

        self._setup_logging()
        self.logger = logging.getLogger(__name__)

        self.exchange = None
        self.strategy = None
        self.running = False

    def _setup_logging(self) -> None:
        """Setup logging with console + rotating file output."""
        log_level = getattr(logging, self.config.get('logging', {}).get('level', 'INFO'))

        fmt = '%(asctime)s | %(name)s | %(levelname)s | %(message)s'
        formatter = logging.Formatter(fmt)

        root = logging.getLogger()
        root.setLevel(log_level)

        # Console handler
        console = logging.StreamHandler()
        console.setLevel(log_level)
        console.setFormatter(formatter)
        root.addHandler(console)

        # File handler
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)

        symbols = self.config.get('symbols', ['UNKNOWN'])
        symbol = symbols[0] if symbols else 'UNKNOWN'

        fh = logging.handlers.RotatingFileHandler(
            log_dir / f'vortex_{symbol}.log',
            maxBytes=10 * 1024 * 1024,
            backupCount=3
        )
        fh.setLevel(log_level)
        fh.setFormatter(formatter)
        root.addHandler(fh)

    def start(self) -> None:
        """Start the bot"""
        strategy_name = self.config.get('strategy', 'vortex_dca').upper()
        self.logger.info(f"=== Starting {strategy_name} Trading Bot ===")
        self.logger.info(f"Symbols: {self.config.get('symbols', [])}")
        self.logger.info(f"Max symbols: {self.config.get('max_symbols', 1)}")

        vortex_config = self.config.get('vortex_dca', {})
        self.logger.info(f"Wallet exposure: {vortex_config.get('wallet_exposure', 0)}%")
        self.logger.info(f"Clusters: {vortex_config.get('nr_clusters', 0)}")

        try:
            # Initialize exchange
            exchange_config = self.config.get('exchange', {})
            exchange_name = exchange_config.get('name', 'bybit')
            ws_enabled = exchange_config.get('websocket_orders', False)
            self.logger.info(f"Connecting to {exchange_name}... (websocket_orders={ws_enabled})")

            if exchange_name == "bybit":
                self.exchange = BybitExchange(
                    config=exchange_config,
                    logger=logging.getLogger('bybit')
                )
            elif exchange_name == "blofin":
                self.exchange = BloFinExchange(
                    config=exchange_config,
                    logger=logging.getLogger('blofin')
                )
            elif exchange_name == "aster":
                self.exchange = AsterExchange(
                    config=exchange_config,
                    logger=logging.getLogger('aster')
                )
            elif exchange_name == "aster_v3":
                self.exchange = AsterExchangeV3(
                    config=exchange_config,
                    logger=logging.getLogger('aster_v3')
                )
            else:
                raise Exception(f"Unsupported exchange: {exchange_name}")

            if not self.exchange.connect():
                raise Exception("Failed to connect to exchange")

            balance = self.exchange.get_balance()
            self.logger.info(f"Wallet balance: ${balance:.2f} USDT")

            if balance < 10:
                raise Exception(f"Insufficient balance: ${balance:.2f} USDT")

            # Initialize strategy
            self.logger.info("Initializing Vortex DCA strategy...")
            self.strategy = VortexDCAStrategy(
                exchange=self.exchange,
                config=self.config,
                logger=logging.getLogger('vortex')
            )

            # Limit symbols
            symbols = self.config.get('symbols', [])
            max_symbols = self.config.get('max_symbols', 1)
            symbols_to_trade = symbols[:max_symbols]

            # Start strategy
            self.strategy.start(symbols_to_trade)
            self.running = True

            self.logger.info(f"{strategy_name} Trading Bot started successfully")

            # Main loop
            self._main_loop()

        except Exception as e:
            self.logger.error(f"Error starting bot: {e}")
            sys.exit(1)

    def stop(self) -> None:
        """Stop the bot"""
        strategy_name = self.config.get('strategy', 'vortex_dca').upper()
        self.logger.info(f"Stopping {strategy_name} Trading Bot...")
        self.running = False

        if self.strategy:
            self.strategy.stop()

        self.logger.info(f"{strategy_name} Trading Bot stopped")

    def _main_loop(self) -> None:
        """Main bot loop"""
        last_status_time = 0

        while self.running:
            try:
                current_time = time.time()

                if current_time - last_status_time >= 60:
                    self._print_status()
                    last_status_time = current_time

                time.sleep(10.0)

            except KeyboardInterrupt:
                self.logger.info("Received interrupt signal")
                break
            except Exception as e:
                self.logger.error(f"Error in main loop: {e}")
                time.sleep(30.0)

    def _print_status(self) -> None:
        """Print bot status"""
        try:
            if not self.strategy:
                return

            status = self.strategy.get_status()
            balance = self.exchange.get_balance()

            strategy_name = self.config.get('strategy', 'vortex_dca').upper()
            self.logger.info(f"=== {strategy_name} Status ===")
            self.logger.info(f"Balance: ${balance:.2f} USDT")
            self.logger.info(f"Active symbols: {status['total_symbols']}")

            for symbol, symbol_status in status['symbols'].items():
                if 'error' in symbol_status:
                    self.logger.warning(f"{symbol}: Error - {symbol_status['error']}")
                    continue

                price = symbol_status.get('current_price', 0)
                position = symbol_status.get('position')

                if position:
                    pnl_pct = position.get('pnl_pct', 0)
                    side = position['side']
                    size = position['size']
                    self.logger.info(
                        f"{symbol}: ${price:.5f} | {side.upper()} {size:.2f} "
                        f"({pnl_pct:+.2f}%)"
                    )
                else:
                    grid_long = symbol_status.get('grid_info', {}).get('long', {})
                    grid_short = symbol_status.get('grid_info', {}).get('short', {})
                    long_levels = grid_long.get('levels', 0)
                    short_levels = grid_short.get('levels', 0)

                    self.logger.info(
                        f"{symbol}: ${price:.5f} | Grid: {long_levels}L/{short_levels}S"
                    )

        except Exception as e:
            self.logger.error(f"Error printing status: {e}")


def signal_handler(signum, frame):
    """Handle shutdown signals"""
    print("\nShutdown signal received")
    global bot
    if 'bot' in globals():
        bot.stop()
    sys.exit(0)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Vortex DCA Trading Bot')
    parser.add_argument(
        '--config',
        type=str,
        default='configs/bybit/vortex/config_vortex_grid_stoploss.json',
        help='Configuration file path'
    )

    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Configuration file not found: {config_path}")
        sys.exit(1)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    global bot
    bot = TradingBot(str(config_path))
    bot.start()


if __name__ == "__main__":
    main()
