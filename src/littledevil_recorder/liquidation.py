"""Public, market-wide USDⓈ-M liquidation order ingestion via `!forceOrder@arr`.

Verified against current official docs (developers.binance.com, Sept 2026):
`!forceOrder@arr` is the all-market liquidation order stream, public and
unauthenticated, distinct from `GET /fapi/v1/forceOrders` (private,
authenticated, user-only liquidation history -- never called here).

Connection contract (current as of Sept 2026): This stream is accessed via
the WebSocket API at `wss://fstream.binance.com/market/stream` with explicit
JSON subscription: `{"method": "SUBSCRIBE", "params": ["!forceOrder@arr"]}`.
The legacy raw-stream path `/ws/!forceOrder@arr` was deprecated April 23, 2026,
and this implementation uses the current /market/stream path.

Per official spec, the stream is explicitly *sampled*: "only the latest one
liquidation order within 1000ms will be pushed as the snapshot." "If no
liquidation happens in the interval of 1000ms, no stream will be pushed."
architecture-review.md §2 independently names this and states the same rule.
Every row is stored with provenance marked as "sampled public liquidation
event," never implied complete.

The stream is single/global, no per-symbol subscribe/unsubscribe. Connection
lifecycle is desired-set-driven only at "any liquidation symbol desired,"
not per symbol; events are filtered against SubscriptionManager's
"liquidation" desired-symbol set before persist/Data Health.

Gap/recovery: no backfill path exists -- Binance publishes no authoritative
historical market-wide liquidation feed (the stream is inherently sampled).
DataHealthTracker's stale(30s)/suspended(120s)/gap_started_at mechanism is
reused unmodified: silence on a desired symbol ages it to explicit gap. A
reconnect alone is not recovery -- only a new `on_liquidation` event clears
the gap, via `record_message` clearing `gap_started_at`.
See tests/test_positioning_liquidation_data_health.py for the proof.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

import websockets

logger = logging.getLogger(__name__)

# Official Binance USDⓈ-M WebSocket API endpoint (current, Sept 2026).
# The legacy /ws/!forceOrder@arr path was deprecated April 23, 2026.
STREAM_HOST = "wss://fstream.binance.com"
STREAM_PATH = "/market/stream"
RECONNECT_DELAY_SECONDS = 2.0
SUBSCRIPTION_REQUEST = {"method": "SUBSCRIBE", "params": ["!forceOrder@arr"], "id": 1}


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
    """Connects to `wss://fstream.binance.com/market/stream` with explicit
    `!forceOrder@arr` subscription (the current official API as of Sept 2026).
    Calls `on_liquidation(parsed)` for every forceOrder event, reconnecting
    on any drop. Runs until `stop_event` is set. The caller filters
    `parsed["symbol"]` against desired symbols -- this stream carries all
    USDⓈ-M symbols and cannot be narrowed server-side."""
    stop_event = stop_event or asyncio.Event()
    url = f"{STREAM_HOST}{STREAM_PATH}"

    while not stop_event.is_set():
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                # Send subscription request per the official WebSocket API contract.
                await ws.send(json.dumps(SUBSCRIPTION_REQUEST))
                logger.info("liquidation stream connected (all-market, /market/stream)")

                while not stop_event.is_set():
                    message = await ws.recv()
                    ts_received = datetime.now(UTC)
                    envelope = json.loads(message)

                    # Skip non-forceOrder messages (e.g., subscription responses).
                    if envelope.get("e") != "forceOrder":
                        continue

                    parsed = parse_liquidation(envelope)
                    parsed["ts_received"] = ts_received
                    await on_liquidation(parsed)
        except (websockets.ConnectionClosed, OSError) as exc:
            logger.warning("liquidation stream dropped (%s); reconnecting in %.1fs", exc, RECONNECT_DELAY_SECONDS)
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)
