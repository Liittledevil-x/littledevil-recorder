import json

import pytest

from littledevil_recorder.subscription_manager import SubscriptionManager


def test_add_symbols_is_idempotent(tmp_path):
    mgr = SubscriptionManager(tmp_path)
    added_first = mgr.add_symbols("trades", ["BTCUSDT", "ETHUSDT"])
    added_second = mgr.add_symbols("trades", ["ETHUSDT", "SOLUSDT"])

    assert added_first == ["BTCUSDT", "ETHUSDT"]
    assert added_second == ["SOLUSDT"]  # ETHUSDT already desired, skipped
    assert mgr.desired_symbols("trades") == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


def test_remove_symbols_is_idempotent(tmp_path):
    mgr = SubscriptionManager(tmp_path)
    mgr.add_symbols("trades", ["BTCUSDT", "ETHUSDT"])

    removed_first = mgr.remove_symbols("trades", ["ETHUSDT", "SOLUSDT"])
    removed_second = mgr.remove_symbols("trades", ["ETHUSDT"])

    assert removed_first == ["ETHUSDT"]  # SOLUSDT was never desired
    assert removed_second == []  # already removed, no-op
    assert mgr.desired_symbols("trades") == ["BTCUSDT"]


def test_symbols_are_uppercased(tmp_path):
    mgr = SubscriptionManager(tmp_path)
    mgr.add_symbols("trades", ["btcusdt"])
    assert mgr.desired_symbols("trades") == ["BTCUSDT"]
    assert mgr.remove_symbols("trades", ["btcusdt"]) == ["BTCUSDT"]


def test_channels_are_independent(tmp_path):
    mgr = SubscriptionManager(tmp_path)
    mgr.add_symbols("trades", ["BTCUSDT", "ETHUSDT", "SOLUSDT"])
    mgr.add_symbols("depth", ["BTCUSDT", "ETHUSDT"])

    assert mgr.desired_symbols("trades") == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    assert mgr.desired_symbols("depth") == ["BTCUSDT", "ETHUSDT"]

    mgr.remove_symbols("depth", ["ETHUSDT"])
    assert mgr.desired_symbols("trades") == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    assert mgr.desired_symbols("depth") == ["BTCUSDT"]


def test_invalid_channel_raises(tmp_path):
    mgr = SubscriptionManager(tmp_path)
    with pytest.raises(ValueError):
        mgr.add_symbols("funding", ["BTCUSDT"])
    with pytest.raises(ValueError):
        mgr.desired_symbols("nonsense")


def test_restart_safe_desired_state_restoration(tmp_path):
    """A fresh SubscriptionManager instance (simulating a process restart)
    must recover exactly the previously persisted desired state."""
    first = SubscriptionManager(tmp_path)
    first.add_symbols("trades", ["BTCUSDT", "ETHUSDT"])
    first.add_symbols("depth", ["BTCUSDT"])
    first.mark_subscribed("trades", ["BTCUSDT", "ETHUSDT"])

    second = SubscriptionManager(tmp_path)
    assert second.desired_symbols("trades") == ["BTCUSDT", "ETHUSDT"]
    assert second.desired_symbols("depth") == ["BTCUSDT"]
    states = {s.symbol: s.status for s in second.states("trades")}
    assert states == {"BTCUSDT": "subscribed", "ETHUSDT": "subscribed"}


def test_persists_atomically_no_leftover_tmp_file(tmp_path):
    mgr = SubscriptionManager(tmp_path)
    mgr.add_symbols("trades", ["BTCUSDT"])

    path = tmp_path / "_local" / "desired_subscriptions.json"
    assert path.exists()
    assert not path.with_suffix(".json.tmp").exists()
    on_disk = json.loads(path.read_text())
    assert on_disk["trades"][0]["symbol"] == "BTCUSDT"


def test_mark_subscribed_and_mark_failed_update_status(tmp_path):
    mgr = SubscriptionManager(tmp_path)
    mgr.add_symbols("trades", ["BTCUSDT", "ETHUSDT"])

    mgr.mark_subscribed("trades", ["BTCUSDT", "ETHUSDT"])
    states = {s.symbol: s.status for s in mgr.states("trades")}
    assert states == {"BTCUSDT": "subscribed", "ETHUSDT": "subscribed"}

    mgr.mark_failed("trades", ["ETHUSDT"], "connection reset")
    states = {s.symbol: (s.status, s.last_error) for s in mgr.states("trades")}
    assert states["BTCUSDT"] == ("subscribed", None)
    assert states["ETHUSDT"] == ("failed", "connection reset")


def test_mark_status_ignores_symbols_removed_since(tmp_path):
    """A stream task that started subscribing to a symbol which was then
    removed before it reported back must not resurrect stale state."""
    mgr = SubscriptionManager(tmp_path)
    mgr.add_symbols("trades", ["BTCUSDT", "ETHUSDT"])
    mgr.remove_symbols("trades", ["ETHUSDT"])

    mgr.mark_subscribed("trades", ["BTCUSDT", "ETHUSDT"])  # stale result for ETHUSDT
    assert mgr.desired_symbols("trades") == ["BTCUSDT"]
    assert [s.symbol for s in mgr.states("trades")] == ["BTCUSDT"]


def test_diff_from_reports_additions_and_removals(tmp_path):
    mgr = SubscriptionManager(tmp_path)
    mgr.add_symbols("trades", ["BTCUSDT", "ETHUSDT", "SOLUSDT"])

    diff = mgr.diff_from("trades", running=["BTCUSDT", "ADAUSDT"])
    assert diff.to_add == ["ETHUSDT", "SOLUSDT"]
    assert diff.to_remove == ["ADAUSDT"]
    assert diff.changed is True


def test_diff_from_no_change_when_running_matches_desired(tmp_path):
    mgr = SubscriptionManager(tmp_path)
    mgr.add_symbols("trades", ["BTCUSDT", "ETHUSDT"])

    diff = mgr.diff_from("trades", running=["ETHUSDT", "BTCUSDT"])  # order-independent
    assert diff.to_add == []
    assert diff.to_remove == []
    assert diff.changed is False


def test_no_duplicate_streams_add_twice_stays_single_entry(tmp_path):
    mgr = SubscriptionManager(tmp_path)
    mgr.add_symbols("trades", ["BTCUSDT"])
    mgr.add_symbols("trades", ["BTCUSDT"])
    mgr.add_symbols("trades", ["btcusdt"])  # same symbol, different case

    assert mgr.desired_symbols("trades") == ["BTCUSDT"]
    assert len(mgr.states("trades")) == 1
