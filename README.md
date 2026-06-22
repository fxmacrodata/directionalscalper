# Directional Scalper / Vortex

Anchored-grid DCA trading bot using the Vortex strategy. Supports perpetual futures on Bybit, BloFin, and Aster (V1 HMAC and V3 API-wallet signing).

## Overview

The Vortex strategy places a DCA grid of limit orders on both sides of the market. Grid spacing adapts dynamically to volatility and orderbook conditions. Core strategy modules (calculator, wave_queue, orderbook_levels, scalper_entry, regime_detector, virtual_chunking_calculator) ship as precompiled `.so` extensions and are not included as Python source.

## Supported Exchanges

| Exchange | Config key | Notes |
|----------|-----------|-------|
| Bybit | `bybit` | WebSocket order support |
| BloFin | `blofin` | Requires `password` (passphrase) |
| Aster V1 | `aster` | HMAC key, legacy endpoint |
| Aster V3 | `aster_v3` | API wallet + EIP-712 signing; requires `eth-account` |

## Requirements

- Requires Python 3.11 (linux-x86_64) — the strategy modules ship as precompiled CPython-3.11 `.so`; other versions must rebuild via `build_strategy.sh`.
- Redis (used by strategy for state persistence)
- See `requirements.txt` for Python dependencies

```bash
pip install -r requirements.txt
```

## Configuration

Copy a template config and fill in your credentials:

```bash
# Bybit example
cp configs/bybit/vortex/config_vortex_grid_stoploss.json configs/bybit/vortex/config_vortex_grid_stoploss.local.json
# Edit the .local.json: set api_key, api_secret, symbols, wallet_exposure, etc.
```

Available templates:

- `configs/bybit/vortex/config_vortex_grid_stoploss.json` - Grid + hard stop-loss
- `configs/bybit/vortex/config_vortex_grid_stoploss_sticky.json` - Sticky DCA variant
- `configs/bybit/vortex/config_vortex_waves_anchored.example.json` - Wave-queue anchor-stable grid
- `configs/bybit/vortex/config_vortex_waves_never_stuck.example.json` - Auto-max-waves rescue ladder
- `configs/bybit/vortex/config_vortex_grid_stoploss_sticky_wavequeue_full.example.json` - Full wave-queue sticky
- `configs/bybit/vortex/config_vortex_walltrap_sticky.example.json` - Wall-trap sticky variant
- `configs/blofin/vortex/` - Equivalent set for BloFin
- `configs/asterdex/vortex/config_vortex_waves_anchored.example.json` - Aster V3 anchor-stable grid

**Never commit `*.local.json` files** - they contain real API keys.

## Running

```bash
python runners/vortex_bot.py --config configs/bybit/vortex/config_vortex_grid_stoploss.local.json
```

The bot logs to `logs/vortex_<SYMBOL>.log` (rotating, 10 MB, 3 backups) and to stdout.

## Edge Modules

The compiled `.so` files for the core strategy logic must be present in `strategies/vortex/` before running. Obtain them from the build pipeline (compiled from Cython/C extensions targeting this platform).
