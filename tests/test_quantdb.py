import os
import pytest
from topquant_ksk.db.quantdb import (
    _make_dsn,
    _service_token_env,
    _tunnel_cmd,
    DEFAULT_HOST,
    DEFAULT_DBNAME,
    DEFAULT_LOCAL_PORT,
    QuantDB,
)


class TestMakeDsn:
    def test_basic_dsn(self):
        dsn = _make_dsn("u", "p", 15432, "quantdb")
        assert dsn == "postgresql://u:p@127.0.0.1:15432/quantdb"

    def test_password_special_chars_quoted(self):
        # '@' 가 들어간 비밀번호도 URL-safe 하게 인코딩되어야 함
        dsn = _make_dsn("shtopquant", "pw@1", 15432, "quantdb")
        assert "pw%401" in dsn
        assert "@127.0.0.1" in dsn  # host 구분자는 1개만


class TestServiceTokenEnv:
    def test_both_present_returns_tunnel_vars(self):
        env = _service_token_env("cid", "csec")
        assert env == {
            "TUNNEL_SERVICE_TOKEN_ID": "cid",
            "TUNNEL_SERVICE_TOKEN_SECRET": "csec",
        }

    def test_missing_returns_empty(self):
        assert _service_token_env(None, None) == {}
        assert _service_token_env("cid", None) == {}
        assert _service_token_env(None, "csec") == {}


class TestTunnelCmd:
    def test_cmd_shape(self):
        cmd = _tunnel_cmd("cf.exe", "h.example", 15432)
        assert cmd == ["cf.exe", "access", "tcp", "--hostname", "h.example", "--url", "127.0.0.1:15432"]


def test_defaults():
    assert DEFAULT_HOST == "shquantdb.alphawaves.vip"
    assert DEFAULT_DBNAME == "quantdb"
    assert DEFAULT_LOCAL_PORT == 15432


class _FakeResult:
    def __init__(self, rows, keys):
        self._rows = rows
        self._keys = keys

    def fetchall(self):
        return self._rows

    def keys(self):
        return self._keys


class _FakeConn:
    def __init__(self, result):
        self.result = result
        self.executed = []

    def execute(self, stmt, params):
        self.executed.append((str(stmt), params))
        return self.result

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeEngine:
    def __init__(self, conn):
        self._conn = conn
        self.disposed = False

    def connect(self):
        return self._conn

    def dispose(self):
        self.disposed = True


class TestQuantDBInit:
    def test_requires_user_and_password(self):
        with pytest.raises(ValueError):
            QuantDB("", "p")
        with pytest.raises(ValueError):
            QuantDB("u", "")

    def test_cf_token_falls_back_to_env(self, monkeypatch):
        monkeypatch.setenv("CF_ACCESS_CLIENT_ID", "env-id")
        monkeypatch.setenv("CF_ACCESS_CLIENT_SECRET", "env-sec")
        db = QuantDB("u", "p")
        assert db.cf_client_id == "env-id"
        assert db.cf_client_secret == "env-sec"

    def test_cf_token_arg_overrides_env(self, monkeypatch):
        monkeypatch.setenv("CF_ACCESS_CLIENT_ID", "env-id")
        db = QuantDB("u", "p", cf_client_id="arg-id", cf_client_secret="arg-sec")
        assert db.cf_client_id == "arg-id"

    def test_defaults_target_quantdb(self):
        db = QuantDB("u", "p")
        assert db.hostname == "shquantdb.alphawaves.vip"
        assert db.dbname == "quantdb"
        assert db.local_port == 15432


class TestEngineProperty:
    def test_engine_outside_context_raises(self):
        db = QuantDB("u", "p")
        with pytest.raises(RuntimeError):
            _ = db.engine


class TestReadSql:
    def test_read_sql_builds_dataframe_and_passes_params(self):
        conn = _FakeConn(_FakeResult([(1, 2), (3, 4)], ["a", "b"]))
        db = QuantDB("u", "p")
        db._engine = _FakeEngine(conn)
        out = db.read_sql("SELECT a, b FROM t WHERE x >= :x", {"x": 10})
        assert list(out.columns) == ["a", "b"]
        assert out.iloc[1]["b"] == 4
        # 바인딩 파라미터가 전달되었는지
        assert conn.executed[0][1] == {"x": 10}

    def test_read_sql_none_params(self):
        conn = _FakeConn(_FakeResult([(5,)], ["v"]))
        db = QuantDB("u", "p")
        db._engine = _FakeEngine(conn)
        out = db.read_sql("SELECT v FROM t")
        assert conn.executed[0][1] == {}
        assert out.iloc[0]["v"] == 5
