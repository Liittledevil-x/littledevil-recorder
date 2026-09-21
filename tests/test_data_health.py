import os
from datetime import UTC, datetime, timedelta

import pytest

psycopg = pytest.importorskip("psycopg")

from littledevil_recorder.data_health import DataHealthTracker

DATABASE_URL = os.getenv("DATABASE_URL")
pytestmark = pytest.mark.skipif(not DATABASE_URL, reason="DATABASE_URL not set")


async def _fetch(conn, channel: str) -> dict:
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT channel, last_message_at, gap_started_at, status FROM data_health WHERE channel = %s",
            (channel,),
        )
        row = await cur.fetchone()
        return row


async def test_new_channel_starts_ok():
    conn = await psycopg.AsyncConnection.connect(
        DATABASE_URL, autocommit=True, row_factory=psycopg.rows.dict_row
    )
    tracker = DataHealthTracker(conn)
    try:
        tracker.record_message("test_channel_ok")
        assert await tracker.flush()
        row = await _fetch(conn, "test_channel_ok")
        assert row["status"] == "ok"
        assert row["gap_started_at"] is None
    finally:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM data_health WHERE channel = %s", ("test_channel_ok",))
        await conn.close()


async def test_sweep_marks_stale_then_suspended_and_recovery_clears_gap():
    conn = await psycopg.AsyncConnection.connect(
        DATABASE_URL, autocommit=True, row_factory=psycopg.rows.dict_row
    )
    tracker = DataHealthTracker(conn)
    try:
        tracker.record_message("test_channel_gap")
        assert await tracker.flush()

        # Force staleness by backdating the in-memory state directly,
        # rather than sleeping 30+ real seconds in a test.
        state = tracker._channels["test_channel_gap"]
        state.last_message_at = datetime.now(UTC) - timedelta(seconds=200)

        tracker.sweep()
        assert await tracker.flush()
        row = await _fetch(conn, "test_channel_gap")
        assert row["status"] == "suspended"
        assert row["gap_started_at"] is not None

        tracker.record_message("test_channel_gap")
        assert await tracker.flush()
        row = await _fetch(conn, "test_channel_gap")
        assert row["status"] == "ok"
        assert row["gap_started_at"] is None
    finally:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM data_health WHERE channel = %s", ("test_channel_gap",))
        await conn.close()
