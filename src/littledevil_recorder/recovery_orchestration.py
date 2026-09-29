"""Orchestrates gap recovery from Data Health signals.

When Data Health detects a gap (channel transitions from 'ok' to 'stale'/'suspended'),
this module queries the database, classifies which gaps are recoverable, invokes
the appropriate recovery adapter (channel-specific), and persists recovery results.

Recovery is coordinated from the periodic_health_sweep task in main.py, not inline
during polling.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
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
) -> list[RecoveryResult]:
    """Query Data Health for unresolved gaps and invoke recovery adapters.

    This is called once per periodic_health_sweep (5-second cadence).
    Each gap is recovered at most once per start; a recovered gap remains
    "recovered" state in Data Health and does not trigger recovery again
    unless manually cleared.

    Returns: list of RecoveryResult objects (empty if no gaps).
    """
    results: list[RecoveryResult] = []

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
            logger.info("gap on %s/%s is unrecoverable (sampled stream); gap remains", channel_type, symbol)
            continue

        # Determine recovery end time: now, or last_message_at if gap is recent
        gap_end = datetime.now(UTC)

        # Skip if gap is too old (older than 30 days) — recovery endpoints have retention windows
        gap_age_days = (gap_end - gap_started_at).days
        if gap_age_days > 30:
            logger.warning("gap on %s/%s is %d days old; exceeds retention window", channel_type, symbol, gap_age_days)
            continue

        logger.info("recovering gap on %s/%s from %s to %s", channel_type, symbol, gap_started_at, gap_end)

        try:
            if channel_type == "positioning":
                # Positioning is composite; recover OI/funding/mark/index/basis sub-channels
                result_oi = await recover_oi_gap(client, writer, symbol, gap_started_at, gap_end)
                results.append(result_oi)
                logger.info("recovered OI: %d rows, resolution=%s", result_oi.recovered_count, result_oi.resolution)

                result_funding = await recover_funding_gap(client, writer, symbol, gap_started_at, gap_end)
                results.append(result_funding)
                logger.info("recovered funding: %d rows, resolution=%s", result_funding.recovered_count, result_funding.resolution)

                result_mark_index = await recover_mark_index_gap(client, writer, symbol, gap_started_at, gap_end)
                results.append(result_mark_index)
                logger.info("recovered mark/index: %d rows, resolution=%s", result_mark_index.recovered_count, result_mark_index.resolution)

                result_basis = await recover_basis_gap(client, writer, symbol, gap_started_at, gap_end)
                results.append(result_basis)
                logger.info("recovered basis: %d rows, resolution=%s", result_basis.recovered_count, result_basis.resolution)

            elif channel_type == "open_interest":
                result = await recover_oi_gap(client, writer, symbol, gap_started_at, gap_end)
                results.append(result)
                logger.info("recovered OI: %d rows, resolution=%s", result.recovered_count, result.resolution)

            elif channel_type == "funding":
                result = await recover_funding_gap(client, writer, symbol, gap_started_at, gap_end)
                results.append(result)
                logger.info("recovered funding: %d rows, resolution=%s", result.recovered_count, result.resolution)

            elif channel_type == "mark_index":
                result = await recover_mark_index_gap(client, writer, symbol, gap_started_at, gap_end)
                results.append(result)
                logger.info("recovered mark/index: %d rows, resolution=%s", result.recovered_count, result.resolution)

            elif channel_type == "basis":
                result = await recover_basis_gap(client, writer, symbol, gap_started_at, gap_end)
                results.append(result)
                logger.info("recovered basis: %d rows, resolution=%s", result.recovered_count, result.resolution)

        except Exception as exc:
            logger.exception("recovery failed for %s/%s: %s", channel_type, symbol, exc)
            continue

    return results
