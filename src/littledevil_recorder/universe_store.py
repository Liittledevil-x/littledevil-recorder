"""Writes Universe Refresh results to universe_membership
(docs/data-and-events.md §1) -- the table replay reads as "universe-as-of"
for any given day (docs/scanner-attention-routing.md §2).
"""

from __future__ import annotations

import json
from datetime import date

import psycopg

from littledevil_recorder.universe import EligibilityCheck


async def record_membership(
    conn: psycopg.AsyncConnection,
    *,
    symbol: str,
    as_of_date: date,
    check: EligibilityCheck,
    eligible: bool,
    consecutive_pass_days: int,
    consecutive_fail_days: int,
    eligibility_basis: str = "full",
) -> None:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO universe_membership
                (symbol, as_of_date, eligible, eligibility_basis, criteria_snapshot,
                 consecutive_pass_days, consecutive_fail_days)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, as_of_date) DO UPDATE SET
                eligible = EXCLUDED.eligible,
                eligibility_basis = EXCLUDED.eligibility_basis,
                criteria_snapshot = EXCLUDED.criteria_snapshot,
                consecutive_pass_days = EXCLUDED.consecutive_pass_days,
                consecutive_fail_days = EXCLUDED.consecutive_fail_days
            """,
            (
                symbol,
                as_of_date,
                eligible,
                eligibility_basis,
                json.dumps(check.criteria_snapshot),
                consecutive_pass_days,
                consecutive_fail_days,
            ),
        )
