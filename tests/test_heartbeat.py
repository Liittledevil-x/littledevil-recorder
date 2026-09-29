"""worker_heartbeats is append-only (trigger-enforced, matching this
codebase's "a frozen decision is never edited after the fact" discipline for
operational tables) - every test below uses a unique instance_id per run and
never deletes rows, following littledevil-api's own
test_worker_heartbeat_integration.py convention.
"""

import asyncio
import os
import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from littledevil_recorder.db import connect_sync
from littledevil_recorder.heartbeat import (
    _publish_heartbeat_sync,
    publish_heartbeats,
    recorder_instance_id,
)

DATABASE_URL = os.getenv("DATABASE_URL")
pytestmark = pytest.mark.skipif(not DATABASE_URL, reason="DATABASE_URL not set")


def _unique_instance_id(label: str) -> str:
    return f"test-recorder-{label}-{uuid.uuid4().hex[:8]}"


def _fetch(conn, instance_id: str) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT service_type, instance_id, current_state, wallet_mode, last_heartbeat_at
               FROM worker_heartbeats WHERE service_type = 'recorder' AND instance_id = %s""",
            (instance_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return {
            "service_type": row[0],
            "instance_id": row[1],
            "current_state": row[2],
            "wallet_mode": row[3],
            "last_heartbeat_at": row[4],
        }


def test_recorder_instance_id_is_hostname_pid_stable_within_process():
    first = recorder_instance_id()
    second = recorder_instance_id()
    assert first == second
    assert "-" in first


def test_publish_heartbeat_sync_creates_a_recorder_row_with_no_wallet_mode():
    conn = connect_sync()
    instance_id = _unique_instance_id("sync-create")
    try:
        _publish_heartbeat_sync(conn, instance_id, "ready")
        row = _fetch(conn, instance_id)
        assert row is not None
        assert row["service_type"] == "recorder"
        assert row["current_state"] == "ready"
        # Recorder is a stateless-w.r.t.-wallet-mode worker (littledevil-shared
        # migrations/0023_worker_heartbeats.sql: "null for stateless workers
        # (API, Recorder)") - never invent a wallet mode for it.
        assert row["wallet_mode"] is None
    finally:
        conn.close()


def test_publish_heartbeat_sync_refreshes_last_heartbeat_at_on_second_call():
    conn = connect_sync()
    instance_id = _unique_instance_id("sync-refresh")
    try:
        _publish_heartbeat_sync(conn, instance_id, "ready")
        first = _fetch(conn, instance_id)
        _publish_heartbeat_sync(conn, instance_id, "ready")
        second = _fetch(conn, instance_id)
        assert second["last_heartbeat_at"] >= first["last_heartbeat_at"]
    finally:
        conn.close()


async def test_publish_heartbeats_publishes_immediately_and_refreshes_periodically():
    instance_id = _unique_instance_id("loop")
    conn = connect_sync()
    try:
        task = asyncio.create_task(publish_heartbeats(instance_id, interval_seconds=0.05))
        try:
            await asyncio.sleep(0.1)
            first = _fetch(conn, instance_id)
            assert first is not None
            await asyncio.sleep(0.15)
            second = _fetch(conn, instance_id)
            assert second["last_heartbeat_at"] >= first["last_heartbeat_at"]
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    finally:
        conn.close()


async def test_publish_heartbeats_survives_a_connection_failure_and_retries(monkeypatch):
    """A publish failure must never propagate out of the task - market
    recording must never pause for any reason (CLAUDE.md non-negotiable),
    and the heartbeat task runs alongside the recording tasks in the same
    asyncio.gather in main.py."""
    instance_id = _unique_instance_id("retry")
    conn = connect_sync()
    calls = {"n": 0}
    real_publish = _publish_heartbeat_sync

    def flaky_publish(c, iid, state):
        calls["n"] += 1
        if calls["n"] == 1:
            raise psycopg.OperationalError("simulated connection drop")
        real_publish(c, iid, state)

    monkeypatch.setattr("littledevil_recorder.heartbeat._publish_heartbeat_sync", flaky_publish)

    try:
        task = asyncio.create_task(publish_heartbeats(instance_id, interval_seconds=0.05))
        try:
            await asyncio.sleep(0.2)
            row = _fetch(conn, instance_id)
            assert row is not None, "heartbeat task must recover after a failed publish attempt"
            assert calls["n"] >= 2
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    finally:
        conn.close()
