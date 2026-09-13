from datetime import UTC, date, datetime

from littledevil_recorder.backfill import (
    AGGTRADE_COLUMNS,
    KLINE_COLUMNS,
    archive_microseconds_to_datetime,
    compute_split_boundary,
    which_block,
)


def test_archive_microseconds_to_datetime_matches_a_real_verified_row():
    # Verified against a real downloaded BTCUSDT-aggTrades-2026-09-01.csv
    # row: the first trade of the day, which must resolve to exactly
    # midnight UTC on that date.
    assert archive_microseconds_to_datetime("1788220800322740") == datetime(
        2026, 9, 1, 0, 0, 0, 322740, tzinfo=UTC
    )


def test_aggtrade_columns_match_the_real_8_column_archive_format():
    # No header row in the real files, and 8 columns including
    # is_best_match -- confirmed against a live download, not memory.
    assert len(AGGTRADE_COLUMNS) == 8
    assert AGGTRADE_COLUMNS[-1] == "is_best_match"


def test_kline_columns_match_the_real_12_column_archive_format():
    assert len(KLINE_COLUMNS) == 12


def test_split_boundary_covers_the_whole_range_with_no_gaps_or_overlap():
    start = date(2025, 9, 14)
    end = date(2026, 9, 13)  # 365 days
    boundary = compute_split_boundary(start, end)

    assert boundary.dev_start == start
    assert boundary.holdout_end == end
    # no gap or overlap between adjacent blocks
    assert (boundary.calibration_start - boundary.dev_end).days == 1
    assert (boundary.holdout_start - boundary.calibration_end).days == 1


def test_split_boundary_approximates_70_15_15():
    start = date(2025, 9, 14)
    end = date(2026, 9, 13)
    total_days = (end - start).days + 1
    boundary = compute_split_boundary(start, end)

    dev_days = (boundary.dev_end - boundary.dev_start).days + 1
    calibration_days = (boundary.calibration_end - boundary.calibration_start).days + 1
    holdout_days = (boundary.holdout_end - boundary.holdout_start).days + 1

    assert dev_days + calibration_days + holdout_days == total_days
    assert abs(dev_days / total_days - 0.70) < 0.01
    assert abs(calibration_days / total_days - 0.15) < 0.01
    assert abs(holdout_days / total_days - 0.15) < 0.02


def test_which_block_classifies_correctly():
    start = date(2025, 9, 14)
    end = date(2026, 9, 13)
    boundary = compute_split_boundary(start, end)

    assert which_block(boundary, boundary.dev_start) == "dev"
    assert which_block(boundary, boundary.dev_end) == "dev"
    assert which_block(boundary, boundary.calibration_start) == "calibration"
    assert which_block(boundary, boundary.holdout_start) == "holdout"
    assert which_block(boundary, boundary.holdout_end) == "holdout"


def test_split_boundary_rejects_too_short_a_range():
    # Discovered via a real 3-day integration test: naive rounding of
    # 70/15/15 over 3 days produces a 0-day calibration block and an
    # inverted (end < start) range. Must raise, not silently corrupt.
    try:
        compute_split_boundary(date(2026, 8, 30), date(2026, 9, 1))
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for a too-short range")


def test_which_block_raises_outside_range():
    start = date(2025, 9, 14)
    end = date(2026, 9, 13)
    boundary = compute_split_boundary(start, end)
    try:
        which_block(boundary, date(2027, 1, 1))
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for out-of-range day")
