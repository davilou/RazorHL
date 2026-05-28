"""Each strategy must populate `indicators_json` on the signal dict so the
fidelity checker can compare live vs backtest indicators exactly."""
import json

import numpy as np
import pandas as pd
import pytest

from bot.strategies.manager import STRATEGY_MAP


def _synth_df(n=300, start_price=100.0, seed=1):
    rng = np.random.default_rng(seed)
    rets = rng.normal(0, 0.005, n)
    closes = start_price * np.exp(np.cumsum(rets))
    highs = closes * (1 + np.abs(rng.normal(0, 0.002, n)))
    lows = closes * (1 - np.abs(rng.normal(0, 0.002, n)))
    ts_ms = np.arange(n) * 300_000 + 1_700_000_000_000
    return pd.DataFrame({"timestamp": ts_ms, "open": closes, "high": highs,
                         "low": lows, "close": closes, "volume": np.ones(n)})


def _make_indicators_stub():
    return {"ema9": 100.0, "ema21": 100.0, "rsi2": 50.0,
            "volume": 1.0, "volume_avg": 1.0,
            "atr": 1.0, "close_1m": 100.0,
            "volume_5m": 1.0, "volume_avg_5m": 1.0, "atr_5m": 1.0}


def _force_signal(strat, df, asset):
    """Try several extreme close perturbations to force a signal from `strat`."""
    sig = strat.evaluate(
        asset=asset, indicators=_make_indicators_stub(), funding_rate=0.0,
        cfg={}, params={}, df_5m=df, new_5m=True,
    )
    if sig is not None:
        return sig
    for mult in (0.80, 1.20, 0.70, 1.30, 0.60, 1.40):
        df2 = df.copy()
        df2.loc[df2.index[-1], "close"] = float(df["close"].iloc[-2]) * mult
        df2.loc[df2.index[-1], "low"] = min(df2["close"].iloc[-1], float(df["low"].iloc[-1]))
        df2.loc[df2.index[-1], "high"] = max(df2["close"].iloc[-1], float(df["high"].iloc[-1]))
        sig = strat.evaluate(
            asset=asset, indicators=_make_indicators_stub(), funding_rate=0.0,
            cfg={}, params={}, df_5m=df2, new_5m=True,
        )
        if sig is not None:
            return sig
    return None


def test_bb_stoch_signal_includes_indicators_json():
    name = next(n for n in STRATEGY_MAP if n.startswith("bb_stoch_"))
    strat = STRATEGY_MAP[name]
    df = _synth_df(n=300)
    asset = (strat.DEFAULT_PARAMS.get("assets") or ["BTC"])[0]
    sig = _force_signal(strat, df, asset)
    assert sig is not None, "Could not force a signal for bb_stoch"
    assert "indicators_json" in sig
    payload = json.loads(sig["indicators_json"])
    assert "bbp" in payload
    assert "stoch_k" in payload
    assert "stoch_d" in payload
    assert "close" in payload


STRATEGY_FAMILY_KEYS = {
    "bb_reversion": ["close", "bbp", "bbm", "bbu", "bbl", "rsi"],
    "bb_rsi":       ["close", "bbp", "bbu", "bbl", "rsi"],
    "stoch_scalp":  ["close", "stoch_k", "stoch_d"],
    "ema_cross":    ["close", "ema_fast", "ema_slow"],
    "macd_cross":   ["close", "macd", "macd_signal"],
    "rsi_scalp":    ["close", "rsi"],
    "williams_r":   ["close", "wr"],
}


@pytest.mark.parametrize("family,required_keys", list(STRATEGY_FAMILY_KEYS.items()))
def test_each_family_emits_indicators_json(family, required_keys):
    name = next((n for n in STRATEGY_MAP if n.startswith(f"{family}_")), None)
    if name is None:
        pytest.skip(f"No registered instance for family {family}")
    strat = STRATEGY_MAP[name]
    df = _synth_df(n=300, seed=hash(family) & 0xFF)
    asset = (strat.DEFAULT_PARAMS.get("assets") or ["BTC"])[0]
    sig = _force_signal(strat, df, asset)
    if sig is None:
        pytest.skip(f"Could not force a signal for {family}")
    assert "indicators_json" in sig, f"{family} signal missing indicators_json"
    payload = json.loads(sig["indicators_json"])
    missing = [k for k in required_keys if k not in payload]
    assert not missing, f"{family} indicators_json missing keys: {missing}"
