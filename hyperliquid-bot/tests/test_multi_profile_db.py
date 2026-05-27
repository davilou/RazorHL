import json
import time

import pytest

from bot import db


def _reset_conn():
    db._local.conn = None


def test_profiles_table_exists(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "test.db")
    _reset_conn()
    db.init_db()
    cols = [r["name"] for r in db.get_conn().execute("PRAGMA table_info(profiles)").fetchall()]
    assert set(cols) >= {
        "id", "name", "exchange",
        "lighter_account_index", "lighter_api_key_private", "lighter_api_key_index",
        "hyperliquid_address", "hyperliquid_secret",
        "created_at", "updated_at",
    }


def test_profile_id_columns_added(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "test.db")
    _reset_conn()
    db.init_db()
    for table in ("trades", "signals", "logs"):
        cols = [r["name"] for r in db.get_conn().execute(f"PRAGMA table_info({table})").fetchall()]
        assert "profile_id" in cols, f"{table} missing profile_id"


def test_m8_creates_default_profile_and_namespaces_keys(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "test.db")
    _reset_conn()
    db.init_db()
    conn = db.get_conn()
    # Seed legacy config that should be namespaced under profile.1.*
    conn.executemany("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", [
        ("strategy.bb_stoch_btc_5m.enabled", "true"),
        ("strategy.bb_stoch_btc_5m.params", json.dumps({"bb_period": 20})),
        ("bot_status", "running"),
        ("assets", json.dumps(["BTC", "ETH"])),
        ("lighter.client_order_counter", "42"),
        ("account_address", "0xabc"),
        ("secret_key", "deadbeef"),
        ("selected_exchange", "lighter"),
        ("risk.max_positions", "3"),
        ("sizing.mode", "risk_pct"),
    ])
    conn.commit()
    # Force re-run of M8 by clearing the marker and any pre-created Default profile
    conn.execute("DELETE FROM config WHERE key = '_migration_multi_profile'")
    conn.execute("DELETE FROM profiles")
    conn.commit()
    db.migrate_db()

    # Default profile created with credentials populated from legacy globals
    row = conn.execute("SELECT * FROM profiles WHERE id = 1").fetchone()
    assert row is not None
    assert row["name"] == "Default"
    assert row["exchange"] == "lighter"
    assert row["hyperliquid_address"] == "0xabc"
    assert row["hyperliquid_secret"] == "deadbeef"

    # Per-profile keys are namespaced
    assert db.get_config("profile.1.strategy.bb_stoch_btc_5m.enabled") == "true"
    assert db.get_config("profile.1.strategy.bb_stoch_btc_5m.params") == json.dumps({"bb_period": 20})
    assert db.get_config("profile.1.bot_status") == "running"
    assert db.get_config("profile.1.assets") == json.dumps(["BTC", "ETH"])
    assert db.get_config("profile.1.lighter.client_order_counter") == "42"
    assert db.get_config("profile.1.risk.max_positions") == "3"
    assert db.get_config("profile.1.sizing.mode") == "risk_pct"

    # Old per-profile keys are deleted from the config table (DEFAULT_CONFIG
    # fallback in get_config would mask this, so query the table directly).
    def _row_exists(key):
        return conn.execute("SELECT 1 FROM config WHERE key = ?", (key,)).fetchone() is not None

    for old in (
        "strategy.bb_stoch_btc_5m.enabled",
        "strategy.bb_stoch_btc_5m.params",
        "bot_status",
        "assets",
        "lighter.client_order_counter",
        "risk.max_positions",
        "sizing.mode",
        # Legacy credential keys consumed into the Default profile row
        "account_address",
        "secret_key",
    ):
        assert not _row_exists(old), f"legacy key {old!r} should be deleted from config table"

    # Truly global keys are preserved
    assert db.get_config("selected_exchange") == "lighter"

    # Marker set
    assert db.get_config("_migration_multi_profile") == "done"


def test_m8_is_idempotent(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "test.db")
    _reset_conn()
    db.init_db()
    db.migrate_db()  # second call must be a no-op
    rows = db.get_conn().execute("SELECT COUNT(*) AS n FROM profiles").fetchone()
    assert rows["n"] >= 1
