#!/usr/bin/env bash
###############################################################################
# build_strategy.sh — Rebuild the Vortex EDGE strategy .so binaries
#
# WHAT THIS DOES
#   Compiles the 6 PROPRIETARY "edge" strategy modules from their private
#   Python sources into binary CPython (Cython) extension modules (.so), and
#   copies ONLY the resulting .so into this public/anchored repo at
#   strategies/vortex/. The real .py sources are NEVER copied here — they stay
#   private inside the private source repo.
#
# IMPORTANT — .so ARE PLATFORM + PYTHON-VERSION SPECIFIC
#   A compiled .so embeds the CPython ABI tag and machine architecture in its
#   filename, e.g.:
#       calculator.cpython-311-x86_64-linux-gnu.so
#                    ^^^^^^      ^^^^^^^^^^^^^^^^^
#                    py3.11      linux x86_64
#   A .so built for CPython 3.11 on linux-x86_64 will NOT import under a
#   different Python minor version (3.10/3.12...) or a different OS/arch
#   (macOS, arm64, musl, etc.). You MUST rebuild on/for every target platform
#   and Python version you intend to run on. The deploy host's `python3
#   --version` and `uname -m` must match the tags in the .so filenames.
#
# WHERE TO RUN THIS
#   Run it INSIDE your PRIVATE source repo checkout, where the real edge .py
#   live (strategies/vortex/*.py). Point DEST at this anchored repo. The
#   script reads the private .py, builds in a scratch dir, and copies the .so
#   out. It never writes back into the private source tree, and it never copies
#   any .py into DEST.
#
# USAGE
#   EDGE_SRC_REPO=/path/to/private-source-repo \
#   DEST=/path/to/ds-vortex-anchored \
#   PYTHON=python3.11 \
#       ./build_strategy.sh
#
#   Defaults (override via env):
#     EDGE_SRC_REPO : auto-detected as the dir containing strategies/vortex/<edge>.py
#     DEST          : directory this script lives in (the anchored repo)
#     PYTHON        : python3.11   (must match your deploy target's Python minor)
#
# REQUIREMENTS on the build host
#   - A C compiler (gcc)             : apt-get install build-essential
#   - Python dev headers for $PYTHON : apt-get install python3.11-dev
#   - Cython + numpy for $PYTHON     : $PYTHON -m pip install cython numpy
###############################################################################
set -euo pipefail

# ---- the 6 EDGE modules (compiled; sources stay private) --------------------
EDGE_MODULES=(
  calculator
  wave_queue
  orderbook_levels
  scalper_entry
  regime_detector
  virtual_chunking_calculator
)

# ---- config (override via environment) --------------------------------------
PYTHON="${PYTHON:-python3.11}"
DEST="${DEST:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"

# Auto-detect the private source repo: the dir holding strategies/vortex/calculator.py
if [[ -z "${EDGE_SRC_REPO:-}" ]]; then
  for guess in "$DEST" "$PWD" "$(dirname "$DEST")"; do
    if [[ -f "$guess/strategies/vortex/calculator.py" ]]; then
      EDGE_SRC_REPO="$guess"; break
    fi
  done
fi
if [[ -z "${EDGE_SRC_REPO:-}" || ! -f "$EDGE_SRC_REPO/strategies/vortex/calculator.py" ]]; then
  echo "ERROR: Could not find the private edge sources." >&2
  echo "       Set EDGE_SRC_REPO to your private source checkout (must contain" >&2
  echo "       strategies/vortex/calculator.py)." >&2
  exit 1
fi

SRC_DIR="$EDGE_SRC_REPO/strategies/vortex"
DEST_DIR="$DEST/strategies/vortex"
EXT_SUFFIX="$("$PYTHON" -c 'import sysconfig; print(sysconfig.get_config_var("EXT_SUFFIX"))')"

echo "Build host Python : $("$PYTHON" --version 2>&1)  (EXT_SUFFIX=$EXT_SUFFIX)"
echo "Edge sources from : $SRC_DIR  (READ-ONLY — never modified)"
echo "Copying .so into  : $DEST_DIR"
echo

# ---- scratch build dir (never touches the private source tree) --------------
BUILD_DIR="$(mktemp -d /tmp/vortex_build.XXXXXX)"
trap 'rm -rf "$BUILD_DIR"' EXIT
mkdir -p "$BUILD_DIR/strategies/vortex"
: > "$BUILD_DIR/strategies/__init__.py"
: > "$BUILD_DIR/strategies/vortex/__init__.py"

# Copy (never move) the private edge .py into the scratch package layout.
for mod in "${EDGE_MODULES[@]}"; do
  cp "$SRC_DIR/$mod.py" "$BUILD_DIR/strategies/vortex/$mod.py"
done

# ---- defensive guard: dead `.clustering` import must fall back to LINEAR -----
# strategies/vortex/clustering/ does not ship publicly. Ensure the import is
# wrapped so a non-LINEAR clustering_algo can never crash at runtime. If the
# private source already guards it (try/except ImportError), this is a no-op.
CAL="$BUILD_DIR/strategies/vortex/calculator.py"
if grep -qE '^[[:space:]]*from \.clustering\.algo_type import AlgoType' "$CAL" \
   && ! grep -qB1 'from .clustering.algo_type import AlgoType' "$CAL" | grep -q 'try:'; then
  "$PYTHON" - "$CAL" <<'PYEOF'
import re, sys
p = sys.argv[1]
s = open(p).read()
needle = "from .clustering.algo_type import AlgoType"
# Only patch the first, unguarded occurrence.
for line in s.splitlines():
    if needle in line:
        indent = line[:len(line) - len(line.lstrip())]
        guarded = (
            f"{indent}try:\n"
            f"{indent}    {needle}\n"
            f"{indent}except ImportError:\n"
            f"{indent}    AlgoType = None  # clustering pkg is private/absent -> LINEAR fallback\n"
        )
        s = s.replace(line + "\n", guarded, 1)
        break
open(p, "w").write(s)
print("  [guard] wrapped dead .clustering import with LINEAR fallback")
PYEOF
fi

# ---- generate setup.py and cythonize ----------------------------------------
cat > "$BUILD_DIR/setup.py" <<'PYEOF'
from setuptools import setup, Extension
from Cython.Build import cythonize
import numpy as np

EDGE_MODULES = [
    "calculator", "wave_queue", "orderbook_levels",
    "scalper_entry", "regime_detector", "virtual_chunking_calculator",
]
extensions = [
    Extension(
        name=f"strategies.vortex.{m}",
        sources=[f"strategies/vortex/{m}.py"],
        include_dirs=[np.get_include()],
    )
    for m in EDGE_MODULES
]
setup(
    name="vortex_edge",
    ext_modules=cythonize(
        extensions,
        compiler_directives={"language_level": "3"},
        build_dir="cython_c",
    ),
    zip_safe=False,
)
PYEOF

(
  cd "$BUILD_DIR"
  "$PYTHON" setup.py build_ext --inplace
)

# ---- copy ONLY the .so into the anchored repo -------------------------------
mkdir -p "$DEST_DIR"
for mod in "${EDGE_MODULES[@]}"; do
  so="$BUILD_DIR/strategies/vortex/$mod$EXT_SUFFIX"
  if [[ ! -f "$so" ]]; then
    echo "ERROR: expected build output missing: $so" >&2
    exit 1
  fi
  cp "$so" "$DEST_DIR/"
  echo "  placed $(basename "$so")"
done

# ---- safety: never leave an edge .py in the anchored repo -------------------
for mod in "${EDGE_MODULES[@]}"; do
  if [[ -f "$DEST_DIR/$mod.py" ]]; then
    echo "ERROR: edge source leaked into anchored repo: $DEST_DIR/$mod.py" >&2
    echo "       Remove it — only .so may live here." >&2
    exit 1
  fi
done

echo
echo "Done. Verify import (no .py should appear below):"
PYTHONPATH="$DEST" "$PYTHON" -c '
from strategies.vortex import (
    calculator, wave_queue, orderbook_levels,
    scalper_entry, regime_detector, virtual_chunking_calculator,
)
for m in (calculator, wave_queue, orderbook_levels, scalper_entry,
          regime_detector, virtual_chunking_calculator):
    assert m.__file__.endswith(".so"), f"NOT a .so: {m.__file__}"
    print("  OK", m.__name__, "->", m.__file__)
print("All 6 edge modules import from .so.")
'
