import asyncio
import csv
import io
import zipfile
from datetime import UTC, date, datetime, timedelta

import httpx
import pyarrow.parquet as pq
import pytest

from littledevil_recorder.historical_corpus import CorpusConfig, HistoricalCorpus


def _archive(rows):
    blob = io.BytesIO()
    with zipfile.ZipFile(blob, "w", zipfile.ZIP_DEFLATED) as archive:
        content = io.StringIO()
        writer = csv.writer(content)
        writer.writerows(rows)
        archive.writestr("rows.csv", content.getvalue())
    return blob.getvalue()


def _rows(first_id, count, start):
    return [
        [first_id + offset, "100.5", "0.25", first_id + offset, first_id + offset,
         str(int((start + timedelta(seconds=offset)).timestamp() * 1_000_000)), "true", "true"]
        for offset in range(count)
    ]


def _client(payloads, calls):
    def handler(request):
        calls.append(str(request.url))
        key = request.url.path.rsplit("aggTrades-", 1)[1].removesuffix(".zip")
        payload = payloads.get(key)
        return httpx.Response(200 if payload else 404, content=payload or b"")

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


@pytest.mark.asyncio
async def test_writes_legacy_engine_compatible_day_and_skips_completed_resume(tmp_path):
    day = date(2026, 1, 2)
    calls = []
    config = CorpusConfig(tmp_path, ("BTCUSDT",), day, day)
    async with _client({day.isoformat(): _archive(_rows(100, 3, datetime(2026, 1, 2, tzinfo=UTC)))}, calls) as client:
        with HistoricalCorpus(config) as corpus:
            first = await corpus.run(client)
            second = await corpus.run(client)
            report = corpus.validate(verify_hashes=True)

    output = tmp_path / "trades" / "BTCUSDT" / "2026-01-02.parquet"
    assert [item.status for item in first] == ["complete"]
    assert [item.status for item in second] == ["skipped"]
    assert len(calls) == 1
    assert pq.ParquetFile(output).metadata.num_rows == 3
    assert report[0]["validation"] == "ok"
    assert report[0]["first_trade_id"] == 100
    assert report[0]["last_trade_id"] == 102


@pytest.mark.asyncio
async def test_failed_write_resumes_from_cached_zip_without_redownloading(tmp_path, monkeypatch):
    day = date(2026, 1, 2)
    calls = []
    config = CorpusConfig(tmp_path, ("BTCUSDT",), day, day)
    async with _client({day.isoformat(): _archive(_rows(100, 2, datetime(2026, 1, 2, tzinfo=UTC)))}, calls) as client:
        with HistoricalCorpus(config) as corpus:
            real_write = corpus._write_day
            monkeypatch.setattr(corpus, "_write_day", lambda *_: (_ for _ in ()).throw(OSError("interrupted")))
            assert (await corpus.run(client))[0].status == "failed"
            monkeypatch.setattr(corpus, "_write_day", real_write)
            assert (await corpus.run(client))[0].status == "complete"

    assert len(calls) == 1
    assert (tmp_path / "source" / "spot" / "aggTrades" / "BTCUSDT" / "2026-01-02.zip").exists()


@pytest.mark.asyncio
async def test_duplicate_ids_inside_one_source_are_rejected_and_never_published(tmp_path):
    day = date(2026, 1, 2)
    rows = _rows(100, 2, datetime(2026, 1, 2, tzinfo=UTC))
    rows.append(rows[-1])
    config = CorpusConfig(tmp_path, ("BTCUSDT",), day, day)
    async with _client({day.isoformat(): _archive(rows)}, []) as client:
        with HistoricalCorpus(config) as corpus:
            result = (await corpus.run(client))[0]
            assert result.status == "complete"
            assert corpus._row("BTCUSDT", day)["source_duplicate_count"] == 1

    assert pq.ParquetFile(tmp_path / "trades" / "BTCUSDT" / "2026-01-02.parquet").metadata.num_rows == 2


@pytest.mark.asyncio
async def test_conflicting_duplicate_id_is_rejected_and_never_published(tmp_path):
    day = date(2026, 1, 2)
    rows = _rows(100, 2, datetime(2026, 1, 2, tzinfo=UTC))
    conflicting = list(rows[-1])
    conflicting[1] = "101.5"
    rows.append(conflicting)
    config = CorpusConfig(tmp_path, ("BTCUSDT",), day, day)
    async with _client({day.isoformat(): _archive(rows)}, []) as client:
        with HistoricalCorpus(config) as corpus:
            result = (await corpus.run(client))[0]
            assert result.status == "failed"
            assert "conflicting duplicate" in result.detail
    assert not (tmp_path / "trades" / "BTCUSDT" / "2026-01-02.parquet").exists()


@pytest.mark.asyncio
async def test_unique_out_of_order_source_ids_are_sorted_before_publish(tmp_path):
    day = date(2026, 1, 2)
    rows = _rows(100, 3, datetime(2026, 1, 2, tzinfo=UTC))
    payload = _archive([rows[2], rows[0], rows[1]])
    config = CorpusConfig(tmp_path, ("BTCUSDT",), day, day)
    async with _client({day.isoformat(): payload}, []) as client:
        with HistoricalCorpus(config) as corpus:
            assert (await corpus.run(client))[0].status == "complete"
            report = corpus.validate()
    assert report[0]["validation"] == "ok"
    assert [row["trade_id"] for row in pq.ParquetFile(tmp_path / "trades" / "BTCUSDT" / "2026-01-02.parquet").read().to_pylist()] == [100, 101, 102]


@pytest.mark.asyncio
async def test_cross_day_overlapping_ids_are_rejected(tmp_path):
    first, second = date(2026, 1, 2), date(2026, 1, 3)
    payloads = {
        first.isoformat(): _archive(_rows(100, 3, datetime(2026, 1, 2, tzinfo=UTC))),
        second.isoformat(): _archive(_rows(102, 3, datetime(2026, 1, 3, tzinfo=UTC))),
    }
    config = CorpusConfig(tmp_path, ("BTCUSDT",), first, second)
    async with _client(payloads, []) as client:
        with HistoricalCorpus(config) as corpus:
            results = await corpus.run(client)

    assert [result.status for result in results] == ["complete", "failed"]
    assert not (tmp_path / "trades" / "BTCUSDT" / "2026-01-03.parquet").exists()


@pytest.mark.asyncio
async def test_corrupt_completed_output_is_quarantined_and_rebuilt_from_cached_source(tmp_path):
    day = date(2026, 1, 2)
    calls = []
    config = CorpusConfig(tmp_path, ("BTCUSDT",), day, day)
    async with _client({day.isoformat(): _archive(_rows(100, 3, datetime(2026, 1, 2, tzinfo=UTC)))}, calls) as client:
        with HistoricalCorpus(config) as corpus:
            assert (await corpus.run(client))[0].status == "complete"
            output = corpus.output_path("BTCUSDT", day)
            output.write_bytes(b"not parquet")
            assert (await corpus.run(client))[0].status == "complete"

    assert len(calls) == 1
    assert pq.ParquetFile(tmp_path / "trades" / "BTCUSDT" / "2026-01-02.parquet").metadata.num_rows == 3
    assert list((tmp_path / "trades" / "BTCUSDT").glob("*.corrupt-*"))


@pytest.mark.asyncio
async def test_interrupted_partial_is_preserved_and_recorded_before_resume(tmp_path):
    day = date(2026, 1, 2)
    config = CorpusConfig(tmp_path, ("BTCUSDT",), day, day)
    output = tmp_path / "trades" / "BTCUSDT" / "2026-01-02.parquet"
    output.parent.mkdir(parents=True)
    stale = output.with_name(f"{output.name}.partial-interrupted")
    stale.write_bytes(b"unfinished")
    async with _client({day.isoformat(): _archive(_rows(100, 2, datetime(2026, 1, 2, tzinfo=UTC)))}, []) as client:
        with HistoricalCorpus(config) as corpus:
            assert (await corpus.run(client))[0].status == "complete"
            artifacts = corpus.partial_artifacts()

    assert len(artifacts) == 1
    assert artifacts[0]["kind"] == "output"
    assert (tmp_path / artifacts[0]["relative_path"]).exists()


def test_root_refuses_a_different_immutable_study_specification(tmp_path):
    with HistoricalCorpus(CorpusConfig(tmp_path, ("BTCUSDT",), date(2026, 1, 1), date(2026, 1, 2))):
        pass
    with pytest.raises(RuntimeError, match="immutable study specification"):
        HistoricalCorpus(CorpusConfig(tmp_path, ("ETHUSDT",), date(2026, 1, 1), date(2026, 1, 2)))
