"""Historical backfill (Stage 0 / phase 0.3, docs/implementation-plan.md):
pulls ~12 months of daily klines + aggTrades from Binance's public archive
for the CoinGecko top-200 by market cap (eligibility hasn't run yet, so
this casts wide -- docs/architecture-review.md §8b), then applies a
time-blocked dev/calibration/holdout split with the boundary logged.

Split ratio (70/15/15 dev/calibration/holdout) is not specified anywhere
in the docs -- every doc says "time-blocked" but none gives a ratio. This
was confirmed with Omar rather than guessed (see dev-journal.md).
"""

from __future__ import annotations

import csv
import io
import zipfile
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import httpx

ARCHIVE_HOST = "https://data.binance.vision"
COINGECKO_HOST = "https://api.coingecko.com/api/v3"

DEV_FRACTION = 0.70
CALIBRATION_FRACTION = 0.15
# HOLDOUT_FRACTION is whatever remains (0.15 here) -- kept implicit so the
# three fractions can never drift out of summing to 1.0.

# Verified against a real downloaded file (2026-09-01), not assumed from
# memory: no header row, 8 columns (Binance's docs omit is_best_match from
# some listings but the archive files include it), and transact_time is in
# MICROSECONDS here -- unlike the live WS stream, which uses milliseconds.
AGGTRADE_COLUMNS = [
    "agg_trade_id", "price", "quantity", "first_trade_id", "last_trade_id",
    "transact_time", "is_buyer_maker", "is_best_match",
]
# Verified the same way: no header row, 12 columns, open_time/close_time
# also in microseconds.
KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume", "close_time",
    "quote_volume", "trade_count", "taker_buy_base_volume", "taker_buy_quote_volume", "ignore",
]


def archive_microseconds_to_datetime(value: str | int) -> datetime:
    return datetime.fromtimestamp(int(value) / 1_000_000, tz=UTC)


async def top_200_symbols(client: httpx.AsyncClient, *, api_key: str | None = None) -> list[str]:
    """CoinGecko top-200 by market cap -> Binance USDT-quoted symbol guesses
    (e.g. 'bitcoin' -> 'BTCUSDT'). Not every CoinGecko coin has a Binance
    Spot USDT pair; callers should treat a 404 on the archive fetch as
    'no Binance listing', not an error."""
    headers = {"x-cg-demo-api-key": api_key} if api_key else {}
    symbols = []
    for page in (1, 2):
        resp = await client.get(
            f"{COINGECKO_HOST}/coins/markets",
            params={
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": 100,
                "page": page,
            },
            headers=headers,
        )
        resp.raise_for_status()
        for coin in resp.json():
            symbols.append(f"{coin['symbol'].upper()}USDT")
    return symbols


async def fetch_daily_aggtrades(client: httpx.AsyncClient, symbol: str, day: date) -> list[dict] | None:
    """Returns None if this symbol/day isn't in the archive (delisted,
    not yet listed, or never had a Binance Spot USDT pair) -- this is an
    expected, non-error outcome given the broad CoinGecko top-200 cast.

    Materializes the whole day as a list -- fine for ad hoc use (tests,
    the live-verification scripts), but backfill_job.py's real run uses
    stream_daily_aggtrades below instead, since a liquid symbol's day is
    1M+ rows and holding every one as a dict (plus the caller's own
    buffered copy) is what OOM-killed the first full-scale run attempt.
    """
    rows = []
    async for row in stream_daily_aggtrades(client, symbol, day):
        if row is None:  # sentinel: symbol/day not in the archive at all
            return None
        rows.append(row)
    return rows


async def stream_daily_aggtrades(client: httpx.AsyncClient, symbol: str, day: date):
    """Yields one row dict at a time instead of materializing the whole
    day, so a caller can write-and-discard each row rather than holding
    a symbol's entire day (1M+ rows for a liquid pair) in memory at once.
    Yields a single `None` and returns if the symbol/day isn't archived."""
    url = f"{ARCHIVE_HOST}/data/spot/daily/aggTrades/{symbol}/{symbol}-aggTrades-{day.isoformat()}.zip"
    resp = await client.get(url)
    if resp.status_code == 404:
        yield None
        return
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        name = zf.namelist()[0]
        with zf.open(name) as f:
            text = io.TextIOWrapper(f, encoding="utf-8")
            for row in csv.reader(text):
                yield dict(zip(AGGTRADE_COLUMNS, row, strict=True))


async def fetch_daily_klines(
    client: httpx.AsyncClient, symbol: str, day: date, timeframe: str = "1h"
) -> list[dict] | None:
    url = f"{ARCHIVE_HOST}/data/spot/daily/klines/{symbol}/{timeframe}/{symbol}-{timeframe}-{day.isoformat()}.zip"
    resp = await client.get(url)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()

    rows = []
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        name = zf.namelist()[0]
        with zf.open(name) as f:
            text = io.TextIOWrapper(f, encoding="utf-8")
            for row in csv.reader(text):
                rows.append(dict(zip(KLINE_COLUMNS, row, strict=True)))
    return rows


@dataclass
class SplitBoundary:
    dev_start: date
    dev_end: date  # inclusive
    calibration_start: date
    calibration_end: date  # inclusive
    holdout_start: date
    holdout_end: date  # inclusive


MIN_DAYS_PER_BLOCK = 1


def compute_split_boundary(start: date, end: date) -> SplitBoundary:
    """Time-blocked dev/calibration/holdout split (architecture-review.md
    §7, §8, §8b) -- dev comes first chronologically, then calibration, then
    holdout, since the holdout must be data the detector parameters were
    never tuned against, in either direction."""
    total_days = (end - start).days + 1
    min_total = MIN_DAYS_PER_BLOCK * 3
    if total_days < min_total:
        raise ValueError(
            f"range is only {total_days} day(s); need at least {min_total} for a "
            "non-degenerate dev/calibration/holdout split"
        )

    dev_days = round(total_days * DEV_FRACTION)
    calibration_days = round(total_days * CALIBRATION_FRACTION)
    # Rounding can push a block below the minimum on a short range (a real
    # ~12-month backfill never hits this; guarded so a caller who passes a
    # short range gets a clear error instead of a silently inverted block).
    dev_days = max(dev_days, MIN_DAYS_PER_BLOCK)
    calibration_days = max(calibration_days, MIN_DAYS_PER_BLOCK)
    holdout_days = total_days - dev_days - calibration_days
    if holdout_days < MIN_DAYS_PER_BLOCK:
        raise ValueError(
            f"range is only {total_days} day(s); the 70/15/15 split leaves fewer than "
            f"{MIN_DAYS_PER_BLOCK} holdout day(s) after rounding"
        )

    dev_end = start + timedelta(days=dev_days - 1)
    calibration_start = dev_end + timedelta(days=1)
    calibration_end = calibration_start + timedelta(days=calibration_days - 1)
    holdout_start = calibration_end + timedelta(days=1)

    return SplitBoundary(
        dev_start=start,
        dev_end=dev_end,
        calibration_start=calibration_start,
        calibration_end=calibration_end,
        holdout_start=holdout_start,
        holdout_end=end,
    )


def which_block(boundary: SplitBoundary, day: date) -> str:
    if boundary.dev_start <= day <= boundary.dev_end:
        return "dev"
    if boundary.calibration_start <= day <= boundary.calibration_end:
        return "calibration"
    if boundary.holdout_start <= day <= boundary.holdout_end:
        return "holdout"
    raise ValueError(f"{day} is outside the boundary's [{boundary.dev_start}, {boundary.holdout_end}] range")
