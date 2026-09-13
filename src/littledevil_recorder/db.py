"""Thin async Postgres access for the recorder's own tables (data_health,
universe_membership). Everything else the recorder writes goes to Parquet
(storage.py) per docs/data-and-events.md §2 -- Postgres here is only for
the small, frequently-updated state tables, not the tick-level archive.
"""

from __future__ import annotations

import os

import psycopg
from psycopg.rows import dict_row


def database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is required")
    return url


async def connect() -> psycopg.AsyncConnection:
    return await psycopg.AsyncConnection.connect(database_url(), row_factory=dict_row, autocommit=True)
