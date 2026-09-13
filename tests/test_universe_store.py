import os
from datetime import date

import pytest

psycopg = pytest.importorskip("psycopg")

from littledevil_recorder.universe import SymbolMetrics, check_eligibility
from littledevil_recorder.universe_store import record_membership

DATABASE_URL = os.getenv("DATABASE_URL")
pytestmark = pytest.mark.skipif(not DATABASE_URL, reason="DATABASE_URL not set")


async def test_record_membership_upserts_and_is_idempotent():
    conn = await psycopg.AsyncConnection.connect(
        DATABASE_URL, autocommit=True, row_factory=psycopg.rows.dict_row
    )
    metrics = SymbolMetrics(
        symbol="TESTUSDT",
        listing_age_days=2000,
        median_quote_volume_30d_usd=1_000_000_000,
        median_spread_bps=1.0,
        p95_spread_bps=3.0,
        median_depth_25bps_usd=5_000_000,
        median_trades_per_minute=500,
        coingecko_rank=1,
        category=None,
        feed_health_7d_pct=99.9,
    )
    check = check_eligibility(metrics)
    today = date(2026, 9, 14)
    try:
        await record_membership(
            conn,
            symbol="TESTUSDT",
            as_of_date=today,
            check=check,
            eligible=True,
            consecutive_pass_days=3,
            consecutive_fail_days=0,
        )
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM universe_membership WHERE symbol = %s AND as_of_date = %s",
                ("TESTUSDT", today),
            )
            row = await cur.fetchone()
        assert row["eligible"] is True
        assert row["consecutive_pass_days"] == 3
        assert row["criteria_snapshot"]["reasons_failed"] == []

        # Re-running the same day should update in place, not duplicate.
        await record_membership(
            conn,
            symbol="TESTUSDT",
            as_of_date=today,
            check=check,
            eligible=False,
            consecutive_pass_days=0,
            consecutive_fail_days=1,
        )
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT count(*) AS n FROM universe_membership WHERE symbol = %s AND as_of_date = %s",
                ("TESTUSDT", today),
            )
            count_row = await cur.fetchone()
        assert count_row["n"] == 1
    finally:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM universe_membership WHERE symbol = %s", ("TESTUSDT",)
            )
        await conn.close()
