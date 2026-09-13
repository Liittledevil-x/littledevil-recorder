#!/usr/bin/env python3
"""Reports actual GB/day from a running recorder's data/ directory, per
Stage 0 gate criterion #4 (docs/architecture-review.md §9: "storage/day
measured"). Run this against a real multi-day recording run, not a short
smoke test -- Parquet's per-file overhead dominates at small sizes and a
few seconds of data does not extrapolate to a meaningful daily rate.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def bytes_per_day(data_root: Path) -> dict[str, int]:
    """Sums file sizes per {date}.parquet filename stem across every
    symbol/kind directory, since a day's total spans every symbol."""
    totals: dict[str, int] = {}
    for path in data_root.glob("*/*/*.parquet"):
        day = path.stem
        totals[day] = totals.get(day, 0) + path.stat().st_size
    return totals


def main() -> None:
    parser = argparse.ArgumentParser(description="Measure recorder storage/day.")
    parser.add_argument("data_root", type=Path)
    args = parser.parse_args()

    if not args.data_root.exists():
        print(f"{args.data_root} does not exist", file=sys.stderr)
        sys.exit(1)

    totals = bytes_per_day(args.data_root)
    if not totals:
        print("no parquet files found under", args.data_root)
        return

    for day, total_bytes in sorted(totals.items()):
        print(f"{day}: {total_bytes / 1_000_000:.2f} MB")

    complete_days = sorted(totals.items())[:-1]  # exclude today, still accumulating
    if complete_days:
        avg = sum(b for _, b in complete_days) / len(complete_days)
        print(f"\naverage over {len(complete_days)} complete day(s): {avg / 1_000_000:.2f} MB/day")
    else:
        print("\nno complete day yet -- only today's partial data exists")


if __name__ == "__main__":
    main()
