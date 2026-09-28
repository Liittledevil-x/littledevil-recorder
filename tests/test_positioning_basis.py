"""Tests for Binance USDⓈ-M basis polling via GET /futures/data/basis."""

from datetime import UTC, datetime

import pytest

from littledevil_recorder.positioning_basis import parse_basis_response
from littledevil_recorder.storage import BASIS_SCHEMA


def test_basis_response_parses_official_contract():
    """Basis endpoint response structure: official USDⓈ-M contract."""
    data = {
        "pair": "BTCUSDT",
        "contractType": "PERPETUAL",
        "basisRate": "0.000123",
        "basis": "50.00",
        "annualizedBasisRate": "0.44955",
        "futuresPrice": "50050.00",
        "indexPrice": "50000.00",
        "timestamp": 1672531200000
    }
    ts_received = datetime.now(UTC)
    result = parse_basis_response("BTCUSDT", data, ts_received)

    assert result["symbol"] == "BTCUSDT"
    assert result["basis_rate"] == 0.000123
    assert result["basis"] == 50.00
    assert result["futures_price"] == 50050.00
    assert result["index_price"] == 50000.00
    assert result["annualized_basis_rate"] == 0.44955
    assert result["contract_type"] == "PERPETUAL"
    assert result["ts_received"] == ts_received


def test_basis_schema_has_official_fields():
    """BASIS_SCHEMA contains all official GET /futures/data/basis response fields."""
    schema_names = [f.name for f in BASIS_SCHEMA]
    assert "ts_exchange" in schema_names
    assert "ts_received" in schema_names
    assert "index_price" in schema_names
    assert "futures_price" in schema_names
    assert "basis_rate" in schema_names
    assert "basis" in schema_names
    assert "annualized_basis_rate" in schema_names
    assert "contract_type" in schema_names
    assert "source" in schema_names
