from bot import db


def _reset_conn():
    db._local.conn = None


def test_m9_adds_indicators_json_to_signals(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "test.db")
    _reset_conn()
    db.init_db()
    cols = [r["name"] for r in db.get_conn().execute("PRAGMA table_info(signals)").fetchall()]
    assert "indicators_json" in cols


def test_m9_is_idempotent(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "test.db")
    _reset_conn()
    db.init_db()
    db.migrate_db()
    cols = [r["name"] for r in db.get_conn().execute("PRAGMA table_info(signals)").fetchall()]
    assert cols.count("indicators_json") == 1
