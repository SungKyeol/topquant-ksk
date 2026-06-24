import os

import pytest

import topquant_ksk.db.quantdb as qd
from topquant_ksk.db.quantdb import (
    _make_dsn,
    _service_token_env,
    _tunnel_cmd,
    DEFAULT_DBNAME,
    DEFAULT_HOSTNAME,
    DEFAULT_LOCAL_PORT,
    QuantDB,
    load_env,
)

# QuantDB.from_env / load_env 가 다루는 env 키 — 단위 테스트는 OS 환경/.env 오염 없이 hermetic 해야 한다.
_QUANTDB_ENV_KEYS = (
    "DB_USER", "DB_PASSWORD", "DB_NAME", "TUNNEL_HOSTNAME", "TUNNEL_PORT",
    "CLOUDFLARED_BIN", "CF_ACCESS_CLIENT_ID", "CF_ACCESS_CLIENT_SECRET",
    "TUNNEL_SERVICE_TOKEN_ID", "TUNNEL_SERVICE_TOKEN_SECRET",
)


@pytest.fixture(autouse=True)
def _clean_env():
    """각 테스트 전후로 QuantDB 관련 env 키를 제거 (OS 환경/.env 영향 차단, 누수 방지)."""
    def _clear():
        for k in _QUANTDB_ENV_KEYS:
            os.environ.pop(k, None)
    _clear()
    yield
    _clear()


def _qdb(**kw):
    """필수 4개 필드를 채운 QuantDB. 테스트별로 일부만 override."""
    base = dict(db_user="u", db_password="p", dbname="quantdb", hostname="shquantdb.alphawaves.vip")
    base.update(kw)
    return QuantDB(**base)


def test_quantdb_exported_from_db_package():
    from topquant_ksk.db import QuantDB as ExportedQuantDB, load_env as ExportedLoadEnv
    from topquant_ksk.db.quantdb import QuantDB as DirectQuantDB
    assert ExportedQuantDB is DirectQuantDB
    assert ExportedLoadEnv is load_env


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


class TestQuantDBInit:
    def test_defaults_applied(self):
        db = QuantDB("u", "p")                  # dbname/hostname 생략 → 기본값
        assert db.db_user == "u" and db.db_password == "p"
        assert db.dbname == DEFAULT_DBNAME == "quantdb"
        assert db.hostname == DEFAULT_HOSTNAME == "shquantdb.alphawaves.vip"
        assert db.local_port == DEFAULT_LOCAL_PORT == 15432
        assert db.cf_client_id is None and db.cf_client_secret is None
        assert db.cloudflared_bin is None       # 미지정 → 런타임 자동탐지

    def test_override_optionals(self):
        db = QuantDB(db_user="u", db_password="p", dbname="d", hostname="h",
                     local_port="25432", cf_client_id="cid", cf_client_secret="csec",
                     cloudflared_bin="/x/cf.exe")
        assert (db.dbname, db.hostname) == ("d", "h")
        assert db.local_port == 25432           # str → int 변환
        assert (db.cf_client_id, db.cf_client_secret, db.cloudflared_bin) == ("cid", "csec", "/x/cf.exe")

    def test_does_not_read_env(self, monkeypatch):
        # 클래스는 env 를 읽지 않는다 — env 가 달라도 기본값/인자만 사용
        monkeypatch.setenv("DB_NAME", "envdb")
        monkeypatch.setenv("TUNNEL_HOSTNAME", "env.host")
        db = QuantDB("u", "p")
        assert db.dbname == "quantdb" and db.hostname == "shquantdb.alphawaves.vip"  # env 무시

    def test_missing_credential_raises_typeerror(self):
        with pytest.raises(TypeError):
            QuantDB("u")               # db_password 누락 → 필수 인자

    def test_empty_credential_raises_valueerror(self):
        with pytest.raises(ValueError):
            QuantDB(db_user="", db_password="p")
        with pytest.raises(ValueError):
            QuantDB(db_user="u", db_password="")

    def test_explicit_none_dbname_raises_valueerror(self):
        with pytest.raises(ValueError) as ei:
            QuantDB(db_user="u", db_password="p", dbname=None, hostname=None)
        assert "dbname" in str(ei.value) and "hostname" in str(ei.value)


class TestEngineProperty:
    def test_engine_outside_context_raises(self):
        db = _qdb()
        with pytest.raises(RuntimeError):
            _ = db.engine


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


class TestReadSql:
    def test_read_sql_builds_dataframe_and_passes_params(self):
        conn = _FakeConn(_FakeResult([(1, 2), (3, 4)], ["a", "b"]))
        db = _qdb()
        db._engine = _FakeEngine(conn)
        out = db.read_sql("SELECT a, b FROM t WHERE x >= :x", {"x": 10}, verbose=False)
        assert list(out.columns) == ["a", "b"]
        assert out.iloc[1]["b"] == 4
        assert conn.executed[0][1] == {"x": 10}  # 바인딩 파라미터 전달 확인

    def test_read_sql_none_params(self):
        conn = _FakeConn(_FakeResult([(5,)], ["v"]))
        db = _qdb()
        db._engine = _FakeEngine(conn)
        out = db.read_sql("SELECT v FROM t", verbose=False)
        assert conn.executed[0][1] == {}
        assert out.iloc[0]["v"] == 5

    def test_read_sql_prints_by_default(self, capsys):
        conn = _FakeConn(_FakeResult([(42,)], ["answer"]))
        db = _qdb()
        db._engine = _FakeEngine(conn)
        db.read_sql("SELECT answer")                 # verbose 기본 True → 자동 print
        out = capsys.readouterr().out
        assert "answer" in out and "42" in out

    def test_read_sql_verbose_false_silent(self, capsys):
        conn = _FakeConn(_FakeResult([(42,)], ["answer"]))
        db = _qdb()
        db._engine = _FakeEngine(conn)
        db.read_sql("SELECT answer", verbose=False)
        assert capsys.readouterr().out == ""


class TestListTables:
    _KEYS = ["schema", "name", "type", "columns"]
    _ROW = ("ai_ready", "etf_daily", "view", "ticker, date, adj_close_pr, adj_close_tr, currency")

    def test_returns_dataframe_and_passes_schema(self):
        conn = _FakeConn(_FakeResult([self._ROW], self._KEYS))
        db = _qdb()
        db._engine = _FakeEngine(conn)
        out = db.list_tables(schema="ai_ready", verbose=False)
        assert list(out.columns) == self._KEYS
        assert out.iloc[0]["name"] == "etf_daily" and out.iloc[0]["type"] == "view"
        assert "ticker" in out.iloc[0]["columns"] and "currency" in out.iloc[0]["columns"]
        assert conn.executed[0][1] == {"schema": "ai_ready"}   # schema 바인딩 전달

    def test_schema_none_default(self):
        conn = _FakeConn(_FakeResult([], self._KEYS))
        db = _qdb()
        db._engine = _FakeEngine(conn)
        db.list_tables(verbose=False)
        assert conn.executed[0][1] == {"schema": None}

    def test_default_prints_full_columns(self, capsys):
        conn = _FakeConn(_FakeResult([self._ROW], self._KEYS))
        db = _qdb()
        db._engine = _FakeEngine(conn)
        db.list_tables()                              # verbose 기본 True → 자동 print
        out = capsys.readouterr().out
        assert "etf_daily" in out
        assert "adj_close_pr" in out and "currency" in out   # 컬럼 잘리지 않고 전부 출력

    def test_verbose_false_silent(self, capsys):
        conn = _FakeConn(_FakeResult([self._ROW], self._KEYS))
        db = _qdb()
        db._engine = _FakeEngine(conn)
        db.list_tables(verbose=False)
        assert capsys.readouterr().out == ""


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

        db = _qdb(cf_client_id="cid", cf_client_secret="csec", tunnel_wait=0)
        proc = db._start_tunnel()

        assert proc.pid == 4321
        assert captured["cmd"] == ["cf.exe", "access", "tcp", "--hostname",
                                   "shquantdb.alphawaves.vip", "--url", "127.0.0.1:15432"]
        assert captured["env"]["TUNNEL_SERVICE_TOKEN_ID"] == "cid"
        assert captured["env"]["TUNNEL_SERVICE_TOKEN_SECRET"] == "csec"

    def test_start_tunnel_no_token_when_absent(self, monkeypatch):
        # _clean_env 픽스처가 TUNNEL_SERVICE_TOKEN_* 를 이미 제거 → 상속 env 에 없음
        captured = {}
        monkeypatch.setattr(qd, "find_cloudflared", lambda: "cf.exe")
        monkeypatch.setattr(qd.subprocess, "Popen",
                            lambda cmd, stdout=None, stderr=None, env=None: captured.update(env=env) or _FakeProc())
        monkeypatch.setattr(qd.time, "sleep", lambda s: None)

        db = _qdb(cf_client_id="", cf_client_secret="", tunnel_wait=0)
        db._start_tunnel()
        assert "TUNNEL_SERVICE_TOKEN_ID" not in captured["env"]
        assert "TUNNEL_SERVICE_TOKEN_SECRET" not in captured["env"]

    def test_explicit_cloudflared_bin_used(self, monkeypatch):
        captured = {}

        def _boom():
            raise AssertionError("find_cloudflared should not be called when cloudflared_bin set")

        monkeypatch.setattr(qd, "find_cloudflared", _boom)
        monkeypatch.setattr(qd.subprocess, "Popen",
                            lambda cmd, stdout=None, stderr=None, env=None: captured.update(cmd=cmd) or _FakeProc())
        monkeypatch.setattr(qd.time, "sleep", lambda s: None)

        db = _qdb(cloudflared_bin="/custom/cf.exe", tunnel_wait=0)
        db._start_tunnel()
        assert captured["cmd"][0] == "/custom/cf.exe"

    def test_falls_back_to_find_cloudflared(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(qd, "find_cloudflared", lambda: "/auto/cf.exe")
        monkeypatch.setattr(qd.subprocess, "Popen",
                            lambda cmd, stdout=None, stderr=None, env=None: captured.update(cmd=cmd) or _FakeProc())
        monkeypatch.setattr(qd.time, "sleep", lambda s: None)

        db = _qdb(tunnel_wait=0)  # cloudflared_bin 미지정 → 자동탐지
        db._start_tunnel()
        assert captured["cmd"][0] == "/auto/cf.exe"

    def test_start_tunnel_missing_cloudflared_raises(self, monkeypatch):
        monkeypatch.setattr(qd, "find_cloudflared", lambda: None)
        db = _qdb(tunnel_wait=0)
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

        with _qdb() as db:
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
            with _qdb() as db:
                raise ValueError("boom")

        assert fake_engine.disposed is True
        assert killed == [True]

    def test_enter_failure_kills_tunnel(self, monkeypatch):
        # 엔진 생성이 __enter__ 에서 실패하면 __exit__ 가 안 돌므로 터널을 직접 정리해야 한다.
        killed = []
        monkeypatch.setattr(QuantDB, "_start_tunnel", lambda self: setattr(self, "_tunnel", _FakeProc()) or self._tunnel)
        monkeypatch.setattr(QuantDB, "_create_verified_engine",
                            lambda self, dsn, **kw: (_ for _ in ()).throw(RuntimeError("engine fail")))
        monkeypatch.setattr(QuantDB, "_kill_tunnel", lambda self: killed.append(True))

        with pytest.raises(RuntimeError):
            with _qdb():
                pass

        assert killed == [True]


class TestLoadEnv:
    def _write(self, tmp_path, body):
        p = tmp_path / ".env"
        p.write_text(body, encoding="utf-8")
        return str(p)

    def test_applies_values(self, tmp_path, monkeypatch):
        monkeypatch.delenv("TQK_TEST_KEY", raising=False)
        path = self._write(tmp_path, "TQK_TEST_KEY=fromfile\n")
        applied = load_env(path=path)
        assert applied.get("TQK_TEST_KEY") == "fromfile"
        assert os.environ["TQK_TEST_KEY"] == "fromfile"

    def test_override_true_dotenv_wins_and_warns(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TQK_TEST_KEY", "fromOS")
        path = self._write(tmp_path, "TQK_TEST_KEY=fromfile\n")
        with pytest.warns(UserWarning, match="충돌"):
            load_env(path=path, override=True)
        assert os.environ["TQK_TEST_KEY"] == "fromfile"   # .env 가 이김

    def test_override_false_os_wins_and_warns(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TQK_TEST_KEY", "fromOS")
        path = self._write(tmp_path, "TQK_TEST_KEY=fromfile\n")
        with pytest.warns(UserWarning, match="충돌"):
            load_env(path=path, override=False)
        assert os.environ["TQK_TEST_KEY"] == "fromOS"      # OS 가 이김

    def test_no_conflict_no_warn(self, tmp_path, monkeypatch, recwarn):
        monkeypatch.setenv("TQK_TEST_KEY", "same")
        path = self._write(tmp_path, "TQK_TEST_KEY=same\n")
        load_env(path=path, override=True, warn_conflicts=True)
        assert not [w for w in recwarn.list if "충돌" in str(w.message)]

    def test_warning_does_not_leak_value(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TQK_TEST_KEY", "os-secret-value")
        path = self._write(tmp_path, "TQK_TEST_KEY=env-secret-value\n")
        with pytest.warns(UserWarning) as rec:
            load_env(path=path, override=True)
        joined = " ".join(str(w.message) for w in rec.list)
        assert "os-secret-value" not in joined and "env-secret-value" not in joined
