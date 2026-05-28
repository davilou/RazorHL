"""End-to-end check that indicators_json survives the live signal path."""
import json

from bot import db


def _reset_conn():
    db._local.conn = None


def test_signal_with_indicators_json_persists_through_insert(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "test.db")
    _reset_conn()
    db.init_db()
    signal = {
        "timestamp": "2026-05-28T12:00:00+00:00", "asset": "BTC", "side": "long",
        "executed": 1, "reason": None,
        "ema9": None, "ema21": None, "rsi2": 0,
        "volume": 1.0, "volume_avg": 1.0, "atr": 100.0, "funding_rate": 0.0,
        "strategy_name": "bb_stoch_btc_5m",
        "indicators_json": json.dumps({"bbp": 0.05, "stoch_k": 12.0, "close": 50000.0}),
    }
    sid = db.insert_signal(signal)
    sigs = db.get_signals(strategy_name="bb_stoch_btc_5m")
    assert any(s["id"] == sid for s in sigs)


def test_get_signals_returns_indicators_json(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "test.db")
    _reset_conn()
    db.init_db()
    db.insert_signal({
        "timestamp": "2026-05-28T12:00:00+00:00", "asset": "BTC", "side": "long",
        "executed": 1, "reason": None,
        "ema9": None, "ema21": None, "rsi2": 0,
        "volume": 1.0, "volume_avg": 1.0, "atr": 100.0, "funding_rate": 0.0,
        "strategy_name": "bb_stoch_btc_5m",
        "indicators_json": '{"bbp": 0.05}',
    })
    sigs = db.get_signals(strategy_name="bb_stoch_btc_5m")
    assert sigs[0]["indicators_json"] == '{"bbp": 0.05}'
