"""Unit tests for trade-layer diff (checker.diff_trades)."""
from bot.fidelity.checker import diff_trades


def _live_trade(entry_ms, side, entry, exit_, pnl, exit_type=None):
    return {"entry_ts_ms": entry_ms, "side": side,
            "entry_price": entry, "exit_price": exit_,
            "pnl": pnl, "exit_type": exit_type}


def _bt_trade(entry_ms, side, entry, exit_, exit_type, duration=10):
    return {"entry_ts_ms": entry_ms, "side": side,
            "entry_price": entry, "exit_price": exit_,
            "exit_type": exit_type, "duration_candles": duration}


def test_exact_match_is_matched():
    live = [_live_trade(1000, "long", 100.0, 101.0, 1.0, "tp")]
    bt = [_bt_trade(1000, "long", 100.0, 101.0, "tp")]
    out = diff_trades(live, bt, tf_ms=300_000)
    assert out["matched"] == 1


def test_extra_live_trade():
    live = [_live_trade(1000, "long", 100.0, 101.0, 1.0)]
    bt = []
    out = diff_trades(live, bt, tf_ms=300_000)
    assert out["extra_live"] == 1


def test_missed_trade():
    live = []
    bt = [_bt_trade(1000, "long", 100.0, 101.0, "tp")]
    out = diff_trades(live, bt, tf_ms=300_000)
    assert out["missed_trade"] == 1


def test_entry_px_drift():
    live = [_live_trade(1000, "long", 100.20, 101.0, 1.0)]   # 0.2% > tol
    bt = [_bt_trade(1000, "long", 100.0, 101.0, "tp")]
    out = diff_trades(live, bt, tf_ms=300_000)
    assert out["entry_px_drift"] == 1


def test_exit_type_mismatch():
    live = [_live_trade(1000, "long", 100.0, 99.0, -1.0, exit_type="sl")]
    bt = [_bt_trade(1000, "long", 100.0, 101.0, "tp")]
    out = diff_trades(live, bt, tf_ms=300_000)
    assert out["exit_type_mismatch"] == 1


def test_match_within_one_candle_window():
    """Live entered 1 candle after backtest — still considered same trade."""
    live = [_live_trade(1_300_000, "long", 100.0, 101.0, 1.0, "tp")]   # 1 candle later
    bt = [_bt_trade(1_000_000, "long", 100.0, 101.0, "tp")]
    out = diff_trades(live, bt, tf_ms=300_000)
    assert out["matched"] == 1
    assert out["missed_trade"] == 0
    assert out["extra_live"] == 0
