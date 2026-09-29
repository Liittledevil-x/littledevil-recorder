"""Canonical persisted worker-heartbeat publisher (littledevil_shared.worker_heartbeat).

Distinct from local_manifest.py's RestartHeartbeat: that is a local,
file-based restart-count log with no Postgres involvement, used to detect a
crash loop even when data_health.status stays 'ok'. This module is the
canonical, cross-service operational heartbeat other services (littledevil-api)
already publish to the shared worker_heartbeats table, so an operator or the
API's /readyz can see Recorder's liveness the same way it sees any other
worker's.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket

import psycopg
from littledevil_shared.worker_heartbeat import WorkerHeartbeatRepository

from littledevil_recorder.db import connect_sync

logger = logging.getLogger(__name__)

DEFAULT_PUBLISH_INTERVAL_SECONDS = 15.0


def recorder_instance_id() -> str:
    """Stable identity for this process's heartbeat row: hostname-pid,
    matching worker_heartbeats.instance_id's documented convention
    (littledevil-shared migrations/0023_worker_heartbeats.sql). A restart
    changes the PID, which is by design: a new instance_id is a new worker
    instance."""
    return f"{socket.gethostname()}-{os.getpid()}"


def publish_interval_seconds() -> float:
    return float(os.environ.get("LITTLEDEVIL_HEARTBEAT_PUBLISH_INTERVAL_SECONDS", str(DEFAULT_PUBLISH_INTERVAL_SECONDS)))


def _publish_heartbeat_sync(conn: psycopg.Connection, instance_id: str, current_state: str) -> None:
    WorkerHeartbeatRepository(conn).publish_heartbeat(
        service_type="recorder",
        instance_id=instance_id,
        process_identity={"hostname": socket.gethostname(), "pid": os.getpid()},
        current_state=current_state,
    )


async def publish_heartbeats(instance_id: str, *, interval_seconds: float | None = None) -> None:
    """Publish once immediately so a fresh process is visible without
    waiting a full interval, then refresh periodically. A connection failure
    (at open or per-publish) is logged and retried next cycle -- it must
    never propagate and take down market recording, which must never pause
    for any reason (CLAUDE.md non-negotiable)."""
    interval = interval_seconds if interval_seconds is not None else publish_interval_seconds()
    conn: psycopg.Connection | None = None
    try:
        while True:
            try:
                if conn is None or conn.closed:
                    conn = await asyncio.to_thread(connect_sync)
                await asyncio.to_thread(_publish_heartbeat_sync, conn, instance_id, "ready")
            except Exception:
                logger.exception("heartbeat publish failed; will retry next interval")
                conn = None
            await asyncio.sleep(interval)
    finally:
        if conn is not None and not conn.closed:
            await asyncio.to_thread(conn.close)
