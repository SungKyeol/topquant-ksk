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


def test_quantdb_exported_from_db_package():
    from topquant_ksk.db import QuantDB as ExportedQuantDB
    from topquant_ksk.db.quantdb import QuantDB as DirectQuantDB
    assert ExportedQuantDB is DirectQuantDB


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


import topquant_ksk.db.quantdb as qd


class _FakeProc:
    def __init__(self, pid=4321):
        self.pid = pid


class TestStartTunnel:
    def test_start_tunnel_injects_service_token_env(self, monkeypatch):
        captured = {}

        def fake_popen(cmd, stdout=None, stderr=None, env=None):
            captured["cmd"] = cmd
            captured["env"] = env
            return _FakeProc()

        monkeypatch.setattr(qd, "find_cloudflared", lambda: "cf.exe")
        monkeypatch.setattr(qd.subprocess, "Popen", fake_popen)
        monkeypatch.setattr(qd.time, "sleep", lambda s: None)

        db = QuantDB("u", "p", cf_client_id="cid", cf_client_secret="csec", tunnel_wait=0)
        proc = db._start_tunnel()

        assert proc.pid == 4321
        assert captured["cmd"] == ["cf.exe", "access", "tcp", "--hostname",
                                   "shquantdb.alphawaves.vip", "--url", "127.0.0.1:15432"]
        assert captured["env"]["TUNNEL_SERVICE_TOKEN_ID"] == "cid"
        assert captured["env"]["TUNNEL_SERVICE_TOKEN_SECRET"] == "csec"

    def test_start_tunnel_no_token_when_absent(self, monkeypatch):
        captured = {}
        # 부모 프로세스 env 오염 방지 (codex finding 3): 상속되는 os.environ 에
        # 토큰 변수가 이미 있으면 안 되므로 명시적으로 제거.
        monkeypatch.delenv("TUNNEL_SERVICE_TOKEN_ID", raising=False)
        monkeypatch.delenv("TUNNEL_SERVICE_TOKEN_SECRET", raising=False)
        monkeypatch.setattr(qd, "find_cloudflared", lambda: "cf.exe")
        monkeypatch.setattr(qd.subprocess, "Popen",
                            lambda cmd, stdout=None, stderr=None, env=None: captured.update(env=env) or _FakeProc())
        monkeypatch.setattr(qd.time, "sleep", lambda s: None)

        db = QuantDB("u", "p", cf_client_id="", cf_client_secret="", tunnel_wait=0)
        db._start_tunnel()
        assert "TUNNEL_SERVICE_TOKEN_ID" not in captured["env"]
        assert "TUNNEL_SERVICE_TOKEN_SECRET" not in captured["env"]

    def test_start_tunnel_missing_cloudflared_raises(self, monkeypatch):
        monkeypatch.setattr(qd, "find_cloudflared", lambda: None)
        db = QuantDB("u", "p", tunnel_wait=0)
        with pytest.raises(RuntimeError):
            db._start_tunnel()


class TestContextManagerLifecycle:
    def test_enter_starts_tunnel_and_engine_exit_cleans_up(self, monkeypatch):
        events = []
        fake_engine = _FakeEngine(_FakeConn(_FakeResult([], [])))

        def fake_start(self):
            events.append("tunnel_start")
            self._tunnel = _FakeProc()
            return self._tunnel

        def fake_verified(self, dsn, **kw):
            events.append(("engine", dsn))
            return fake_engine

        def fake_kill(self):
            events.append("tunnel_kill")
            self._tunnel = None

        monkeypatch.setattr(QuantDB, "_start_tunnel", fake_start)
        monkeypatch.setattr(QuantDB, "_create_verified_engine", fake_verified)
        monkeypatch.setattr(QuantDB, "_kill_tunnel", fake_kill)

        with QuantDB("u", "p") as db:
            assert db.engine is fake_engine

        assert events[0] == "tunnel_start"
        assert events[1][0] == "engine"
        assert events[1][1] == "postgresql://u:p@127.0.0.1:15432/quantdb"
        assert "tunnel_kill" in events
        assert fake_engine.disposed is True

    def test_exit_cleans_up_on_exception(self, monkeypatch):
        fake_engine = _FakeEngine(_FakeConn(_FakeResult([], [])))
        monkeypatch.setattr(QuantDB, "_start_tunnel", lambda self: setattr(self, "_tunnel", _FakeProc()) or self._tunnel)
        monkeypatch.setattr(QuantDB, "_create_verified_engine", lambda self, dsn, **kw: fake_engine)
        killed = []
        monkeypatch.setattr(QuantDB, "_kill_tunnel", lambda self: killed.append(True))

        with pytest.raises(ValueError):
            with QuantDB("u", "p") as db:
                raise ValueError("boom")

        assert fake_engine.disposed is True
        assert killed == [True]
