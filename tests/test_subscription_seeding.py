"""recover_and_run seeds SubscriptionManager desired state from env-var
symbol lists only on a genuinely first-ever start; once desired state has
been persisted (by an earlier run, or a runtime add/remove), a restart must
restore that persisted state and never silently re-seed from a stale env
var -- exactly the "restart-safe desired-state restoration" requirement.
"""

from __future__ import annotations

from littledevil_recorder.subscription_manager import SubscriptionManager


def _seed_like_recover_and_run(subscriptions: SubscriptionManager, trade_symbols, depth_symbols):
    """Mirrors main.recover_and_run's seeding branch exactly, so this test
    exercises the same logic without needing the full async server."""
    if not subscriptions.desired_symbols("trades") and not subscriptions.desired_symbols("depth"):
        subscriptions.add_symbols("trades", trade_symbols)
        subscriptions.add_symbols("depth", depth_symbols)
    return subscriptions.desired_symbols("trades"), subscriptions.desired_symbols("depth")


def test_first_start_seeds_from_env_var_symbols(tmp_path):
    subscriptions = SubscriptionManager(tmp_path)
    trades, depth = _seed_like_recover_and_run(subscriptions, ["BTCUSDT", "ETHUSDT"], ["BTCUSDT"])

    assert trades == ["BTCUSDT", "ETHUSDT"]
    assert depth == ["BTCUSDT"]


def test_restart_ignores_env_var_once_state_is_persisted(tmp_path):
    first = SubscriptionManager(tmp_path)
    _seed_like_recover_and_run(first, ["BTCUSDT", "ETHUSDT"], ["BTCUSDT"])
    first.add_symbols("trades", ["SOLUSDT"])  # a runtime addition after seeding

    # Simulate a process restart with the OLD env var symbol list (as if the
    # deploy config hadn't been updated) plus a fresh SubscriptionManager
    # instance reading the same data root.
    second = SubscriptionManager(tmp_path)
    trades, depth = _seed_like_recover_and_run(second, ["BTCUSDT", "ETHUSDT"], ["BTCUSDT"])

    # SOLUSDT (added at runtime) must survive the restart; the env var list
    # must not have been re-applied and must not have dropped it.
    assert trades == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    assert depth == ["BTCUSDT"]


def test_restart_after_runtime_removal_does_not_resurrect_symbol(tmp_path):
    first = SubscriptionManager(tmp_path)
    _seed_like_recover_and_run(first, ["BTCUSDT", "ETHUSDT"], [])
    first.remove_symbols("trades", ["ETHUSDT"])

    second = SubscriptionManager(tmp_path)
    trades, _ = _seed_like_recover_and_run(second, ["BTCUSDT", "ETHUSDT"], [])

    # ETHUSDT was explicitly removed at runtime; a restart with the original
    # env var list must not bring it back.
    assert trades == ["BTCUSDT"]
