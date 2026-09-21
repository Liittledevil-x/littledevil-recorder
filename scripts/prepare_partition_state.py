#!/usr/bin/env python3
"""Prebuild the append-parts trade-ID sidecar before a controlled cutover.

This streams only existing legacy trade IDs.  It never changes a Parquet file
or service configuration, and is idempotent, so it can be run and verified
while the legacy recorder remains online before its planned final restart.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from littledevil_recorder.storage import bootstrap_legacy_trade_index


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--symbols", required=True, help="comma-separated Binance symbols")
    args = parser.parse_args()
    symbols = [symbol.strip().upper() for symbol in args.symbols.split(",") if symbol.strip()]
    bootstrap_legacy_trade_index(args.data_root, symbols)
    print(f"prepared local trade-ID index for {len(symbols)} symbol(s)")


if __name__ == "__main__":
    main()
