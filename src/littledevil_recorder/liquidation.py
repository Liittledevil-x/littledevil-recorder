"""Public, market-wide USDⓈ-M liquidation order ingestion via `!forceOrder@arr`.

Verified against the current official docs (developers.binance.com, USDⓈ-M
futures websocket market streams): `!forceOrder@arr` is the all-market
liquidation order stream, public and unauthenticated, distinct in every way
from `GET /fapi/v1/forceOrders` (a *private*, API-key-authenticated endpoint
that returns only the caller's own liquidation history). This module never
calls, and must never be made to call, that private endpoint -- there is no
legitimate way to substitute one's own trade history for market-wide data,
and doing so would silently misrepresent a single account's liquidations as
the market's.

Per the official spec, the stream is explicitly *sampled*, not a complete
liquidation log: "for each symbol, only the latest one liquidation order
within 1000ms will be pushed as the snapshot." architecture-review.md §2
independently names this exact stream and states the same sampling rule
("one sampled order per symbol per 1000 ms -- store as *sampled*"); every
persisted row is written with that provenance, never implied to be complete.

The stream is single, global, and carries every symbol at once -- there is
no per-symbol subscribe/unsubscribe on `!forceOrder@arr`, unlike the spot
@aggTrade/@depth combined streams this recorder otherwise uses. Connection
lifecycle is therefore desired-set-driven only at the granularity of "any
liquidation symbol desired at all", not per symbol; each event is filtered
against SubscriptionManager's "liquidation" desired-symbol set before being
persisted, and only accepted symbols count toward Data Health.

Host: `wss://fstream.binance.com`, the USDⓈ-M futures websocket base --
distinct from the spot-only `wss://data-stream.binance.vision` this recorder
uses for @aggTrade/@depth (architecture-review.md §2 keeps Spot as primary
for trade/depth and USDⓈ-M strictly for positioning/liquidation context).

Gap/recovery semantics: there is no backfill path for a missed liquidation
interval, and there cannot be one -- Binance itself does not publish a
complete historical liquidation feed (the stream is explicitly a sampled
snapshot even while healthy: "only the latest one ... within 1000ms" per its
own docs), so there is no authoritative source to re-fetch a gap from even in
principle. A stalled/dropped connection is therefore handled purely as a
detection problem, reusing DataHealthTracker's existing stale (30s) /
suspended (120s) / gap_started_at mechanism unmodified: silence on a desired
symbol's channel ages it to an explicit gap exactly as positioning does (see
positioning_poller.py's own gap/recovery note for the identical reasoning
applied to polling). A reconnect of the underlying websocket is not treated
as proof the missed interval was recovered -- only a new `on_liquidation`
call for that symbol clears the gap, which is the natural consequence of
`record_message` being the only thing that clears `gap_started_at`.
See tests/test_positioning_liquidation_data_health.py for the interference
proof (stall -> suspended -> explicit gap -> new event clears it).
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

import websockets

logger = logging.getLogger(__name__)

STREAM_HOST = "wss://fstream.binance.com"
STREAM_PATH = "/ws/!forceOrder@arr"
RECONNECT_DELAY_SECONDS = 2.0


def parse_liquidation(raw: dict) -> dict:
    """Parse one `forceOrder` event payload into normalized fields.

    `raw["o"]` carries the order fields; `raw["E"]` is the event time and is
    used as ts_exchange (the stream's own authoritative event time, distinct
    from `o.T`, the order's own trade time, which is preserved separately as
    order_trade_time).
    """
    order = raw["o"]
    return {
        "symbol": order["s"],
        "ts_exchange": datetime.fromtimestamp(raw["E"] / 1000, tz=UTC),
        "side": order["S"],
        "order_type": order["o"],
        "time_in_force": order["f"],
        "orig_qty": float(order["q"]),
        "price": float(order["p"]),
        "avg_price": float(order["ap"]),
        "order_status": order["X"],
        "last_filled_qty": float(order["l"]),
        "accumulated_qty": float(order["z"]),
        "order_trade_time": datetime.fromtimestamp(order["T"] / 1000, tz=UTC),
    }


OnLiquidation = Callable[[dict], Awaitable[None]]


async def run_liquidation_stream(
    on_liquidation: OnLiquidation,
    *,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Connects to the single all-market `!forceOrder@arr` stream and calls
    `on_liquidation(parsed)` for every message, reconnecting on any drop.
    Runs until `stop_event` is set (or forever if none given). The caller is
    responsible for filtering `parsed["symbol"]` against whatever symbol set
    it cares about -- this stream carries every USDⓈ-M symbol's liquidations
    and cannot be narrowed server-side."""
    stop_event = stop_event or asyncio.Event()
    url = f"{STREAM_HOST}{STREAM_PATH}"

    while not stop_event.is_set():
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                logger.info("liquidation stream connected (all-market)")
                while not stop_event.is_set():
                    message = await ws.recv()
                    ts_received = datetime.now(UTC)
                    envelope = json.loads(message)
                    parsed = parse_liquidation(envelope)
                    parsed["ts_received"] = ts_received
                    await on_liquidation(parsed)
        except (websockets.ConnectionClosed, OSError) as exc:
            logger.warning("liquidation stream dropped (%s); reconnecting in %.1fs", exc, RECONNECT_DELAY_SECONDS)
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)
