"""Orchestrates gap recovery from Data Health signals.

When Data Health detects a gap (channel transitions from 'ok' to 'stale'/'suspended'),
this module queries the database, classifies which gaps are recoverable, invokes
the appropriate recovery adapter (channel-specific), and persists recovery results.

Recovery is coordinated from the periodic_health_sweep task in main.py, not inline
during polling.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime

import httpx
import psycopg

from littledevil_recorder.data_health import DataHealthTracker
from littledevil_recorder.positioning_recovery import (
    RecoveryResult,
    recovery_capability,
    recover_basis_gap,
    recover_funding_gap,
    recover_mark_index_gap,
    recover_oi_gap,
)
from littledevil_recorder.storage import ParquetWriter

logger = logging.getLogger(__name__)


async def orchestrate_recovery_from_gaps(
    health: DataHealthTracker,
    writer: ParquetWriter,
    conn: psycopg.AsyncConnection,
    client: httpx.AsyncClient,
    *,
    handled_recovery_parts: set[tuple[str, str, datetime, str]] | None = None,
) -> list[RecoveryResult]:
    """Query Data Health for unresolved gaps and invoke recovery adapters.

    This is called once per periodic_health_sweep (5-second cadence).
    Each gap is recovered at most once per start; a recovered gap remains
    "recovered" state in Data Health and does not trigger recovery again
    unless manually cleared.

    Returns: list of RecoveryResult objects (empty if no gaps).
    """
    results: list[RecoveryResult] = []
    handled = handled_recovery_parts if handled_recovery_parts is not None else set()

    try:
        async with conn.cursor() as cur:
            # Query all channels with unresolved gaps (status != 'ok' and not yet recovered)
            await cur.execute("""
                SELECT channel, last_message_at, gap_started_at, status
                FROM data_health
                WHERE status IN ('stale', 'suspended')
                  AND (gap_started_at IS NOT NULL)
                ORDER BY channel
            """)
            rows = await cur.fetchall()
    except Exception as exc:
        logger.error("failed to query data_health for gaps: %s", exc)
        return results

    if not rows:
        return results

    for row in rows:
        if isinstance(row, Mapping):
            channel = row["channel"]
            last_message_at = row["last_message_at"]
            gap_started_at = row["gap_started_at"]
            status = row["status"]
        else:
            channel, last_message_at, gap_started_at, status = row
        # Parse channel name: binance_{channel_type}_{symbol}
        # Examples: binance_positioning_BTCUSDT, binance_liquidation_ETHUSDT
        parts = channel.split("_", 2)
        if len(parts) != 3 or parts[0] != "binance":
            logger.warning("skipping malformed channel name: %s", channel)
            continue

        channel_type = parts[1]
        symbol = parts[2]

        # Classify recovery capability
        capability = recovery_capability(channel_type)
        if capability is None:
            logger.warning("unknown channel type for recovery: %s", channel_type)
            continue

        if capability == "unrecoverable":
            key = (channel_type, symbol, gap_started_at, "unrecoverable")
            if key not in handled:
                logger.info("gap on %s/%s is unrecoverable (sampled stream); gap remains", channel_type, symbol)
                handled.add(key)
            continue

        # Determine recovery end time: now, or last_message_at if gap is recent
        gap_end = datetime.now(UTC)

        # Skip if gap is too old (older than 30 days) — recovery endpoints have retention windows
        gap_age_days = (gap_end - gap_started_at).days
        if gap_age_days > 30:
            logger.warning("gap on %s/%s is %d days old; exceeds retention window", channel_type, symbol, gap_age_days)
            continue

        part_names = {
            "positioning": ("open_interest", "funding", "mark_index", "basis"),
            "open_interest": ("open_interest",),
            "funding": ("funding",),
            "mark_index": ("mark_index",),
            "basis": ("basis",),
        }.get(channel_type, ())
        if part_names and all(
            (channel_type, symbol, gap_started_at, part) in handled
            for part in part_names
        ):
            continue
        logger.info("recovering gap on %s/%s from %s to %s", channel_type, symbol, gap_started_at, gap_end)

        async def run_part(
            part: str,
            label: str,
            operation: Callable[[], Awaitable[RecoveryResult]],
        ) -> None:
            key = (channel_type, symbol, gap_started_at, part)
            if key in handled:
                return
            result = await operation()
            results.append(result)
            logger.info("recovered %s: %d rows, resolution=%s", label, result.recovered_count, result.resolution)
            if not result.errors:
                handled.add(key)

        try:
            if channel_type == "positioning":
                # Positioning is composite; recover OI/funding/mark/index/basis sub-channels
                await run_part(
                    "open_interest", "OI",
                    lambda: recover_oi_gap(client, writer, symbol, gap_started_at, gap_end),
                )
                await run_part(
                    "funding", "funding",
                    lambda: recover_funding_gap(client, writer, symbol, gap_started_at, gap_end),
                )
                await run_part(
                    "mark_index", "mark/index",
                    lambda: recover_mark_index_gap(client, writer, symbol, gap_started_at, gap_end),
                )
                await run_part(
                    "basis", "basis",
                    lambda: recover_basis_gap(client, writer, symbol, gap_started_at, gap_end),
                )

            elif channel_type == "open_interest":
                await run_part(
                    "open_interest", "OI",
                    lambda: recover_oi_gap(client, writer, symbol, gap_started_at, gap_end),
                )

            elif channel_type == "funding":
                await run_part(
                    "funding", "funding",
                    lambda: recover_funding_gap(client, writer, symbol, gap_started_at, gap_end),
                )

            elif channel_type == "mark_index":
                await run_part(
                    "mark_index", "mark/index",
                    lambda: recover_mark_index_gap(client, writer, symbol, gap_started_at, gap_end),
                )

            elif channel_type == "basis":
                await run_part(
                    "basis", "basis",
                    lambda: recover_basis_gap(client, writer, symbol, gap_started_at, gap_end),
                )

        except Exception as exc:
            logger.exception("recovery failed for %s/%s: %s", channel_type, symbol, exc)
            continue

    return results
