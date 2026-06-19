# QuantDB DB 접근 경로 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `topquant_ksk.db` 에 컨텍스트매니저 기반 신규 클래스 `QuantDB` 를 추가하여 quantdb 배포본(ai_ready 스키마, CF Access 서비스토큰)에 깔끔하게 접속하고, `series_kodex_spy_krw_tunnel.py` 의 터널/엔진 보일러플레이트를 제거한다.

**Architecture:** 새 파일 `src/topquant_ksk/db/quantdb.py` 에 순수 헬퍼(`_make_dsn`/`_service_token_env`/`_tunnel_cmd`) + `QuantDB` 클래스(컨텍스트매니저: `__enter__` 에서 cloudflared 터널 1회 + 검증된 SQLAlchemy 엔진 생성, `__exit__` 에서 정리). `read_sql(sql, params)` → pandas.DataFrame, `.engine` escape hatch 노출. 기존 `DBConnection`/fetcher/`manage_db_tunnel` 은 0 변경(legacy 동결). 시크릿은 전부 `.env`/env 에서만 주입.

**Tech Stack:** Python 3.10+, SQLAlchemy, pandas, cloudflared(외부 exe), pytest. (Anaconda Python: `C:/ProgramData/anaconda3/python.exe`)

## Global Constraints

- Python 인터프리터: `C:/ProgramData/anaconda3/python.exe`. 테스트/실행 시 `PYTHONIOENCODING=utf-8` 설정 (Windows cp949 회피).
- import 문은 항상 파일 맨 위 (함수 내부 import 금지) — 프로젝트 규칙.
- 시크릿(DB 비밀번호, CF 서비스토큰)은 라이브러리 코드·repo 에 **절대** 하드코딩 금지. 코드는 env 변수 *이름*(`DB_USER`/`DB_PASSWORD`/`CF_ACCESS_CLIENT_ID`/`CF_ACCESS_CLIENT_SECRET`)만 참조.
- SQL 파라미터는 SQLAlchemy `text()` 바인딩 사용 (문자열 포매팅 금지).
- 비밀번호는 DSN 에 넣기 전 `urllib.parse.quote_plus` 적용.
- 반환은 pandas DataFrame.
- QuantDB 기본 타깃 = quantdb (`hostname="shquantdb.alphawaves.vip"`, `dbname="quantdb"`). host/dbname 은 시크릿 아니므로 기본값으로 박아도 됨.
- 기존 quant_data 경로(`DBConnection`, `download.py`, `upload.py`, `tools.py`, `tunnel.manage_db_tunnel`)는 건드리지 않는다.
- 배경: `CONTEXT.md`, `docs/adr/0001-quantdb-forward-db-path.md`.

## File Structure

- `src/topquant_ksk/db/quantdb.py` — **신규.** 순수 헬퍼 + `QuantDB` 클래스. `tunnel.find_cloudflared` 재사용.
- `src/topquant_ksk/db/__init__.py` — **수정.** `from .quantdb import QuantDB` export 추가.
- `series_kodex_spy_krw_tunnel.py` — **수정(재작성).** QuantDB 사용, 시크릿 env, 터널/엔진 보일러플레이트 제거. `build()` 를 `db` 인자로 리팩터(테스트 가능 seam).
- `.env.example` — **신규.** 필요한 env 키 문서화 (commit 가능, 실제 `.env` 는 gitignore).
- `tests/test_quantdb.py` — **신규.** 순수 헬퍼 + read_sql(fake engine) + 컨텍스트매니저(monkeypatch) 단위테스트.
- `tests/test_series_kodex_spy_krw.py` — **신규.** `build()` 의 merge_asof/파생 로직을 fake db 로 단위테스트.

## Setup (한 번만)

- [ ] **브랜치 생성** (worktree 아님 — repo 에 `.worktrees/` 없음):

```
git checkout -b feat/quantdb-db-access
```

---

### Task 0: 테스트 인프라 — CI 에 `[db]` 설치 + python-dotenv 선언

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `pyproject.toml`

**이유:** 신규 테스트가 `from topquant_ksk.db...` 를 import 하면 `db/__init__.py` 가 `upload`(xlwings)·`download`(polars) 를 끌어온다. CI 는 현재 `.[plot]` 만 설치하므로 `import topquant_ksk.db` 가 ImportError → 신규 테스트 collection 실패. 또한 재작성된 스크립트는 `.env` 로딩에 `python-dotenv` 를 쓰는데 어느 의존성 그룹에도 선언돼 있지 않다 (codex finding 2).

- [ ] **Step 1: ci.yml 설치 라인 수정**

`.github/workflows/ci.yml` 의 install 단계:

```yaml
      - name: Install package and test dependencies
        run: |
          pip install -e ".[plot,db]"
          pip install pytest pytest-cov ruff
```

(변경점: `".[plot]"` → `".[plot,db]"`. 비고: `xlwings`/`polars`/`psycopg2-binary`/`connectorx` 는 ubuntu 에서 import-가능하게 설치된다. 만약 ubuntu CI 에서 `import xlwings` 가 실패하면 — 대안: `db/__init__.py` 의 `from . import upload`/`download` 를 lazy/guarded import 로 바꾸는 별도 작업이 필요하나, 그건 기존 모듈 변경이므로 이 계획 범위 밖. 우선 `[db]` 설치로 진행.)

- [ ] **Step 2: pyproject `[db]` 에 python-dotenv 추가**

`pyproject.toml`:

```toml
db = ["xlwings", "polars", "sqlalchemy", "psycopg2-binary", "connectorx", "python-dotenv"]
```

- [ ] **Step 3: 로컬 확인**

Run: `& C:/ProgramData/anaconda3/python.exe -m pip install -e ".[plot,db]"`
그 후 Run: `$env:PYTHONIOENCODING='utf-8'; & C:/ProgramData/anaconda3/python.exe -c "import topquant_ksk.db; print('ok')"`
Expected: `ok` (ImportError 없이)

- [ ] **Step 4: 커밋**

```
git add .github/workflows/ci.yml pyproject.toml
git commit -m "build: install [db] in CI and add python-dotenv to [db] extra"
```

---

### Task 1: QuantDB 순수 헬퍼 (DSN / 서비스토큰 env / 터널 cmd)

**Files:**
- Create: `src/topquant_ksk/db/quantdb.py`
- Test: `tests/test_quantdb.py`

**Interfaces:**
- Consumes: `topquant_ksk.db.tunnel.find_cloudflared` (기존)
- Produces:
  - `_make_dsn(db_user: str, db_password: str, local_port: int, dbname: str) -> str`
  - `_service_token_env(cf_client_id: str | None, cf_client_secret: str | None) -> dict`
  - `_tunnel_cmd(cloudflared_exe: str, hostname: str, local_port: int) -> list[str]`
  - 모듈 상수: `DEFAULT_HOST="shquantdb.alphawaves.vip"`, `DEFAULT_DBNAME="quantdb"`, `DEFAULT_LOCAL_PORT=15432`

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/test_quantdb.py`:

```python
from topquant_ksk.db.quantdb import (
    _make_dsn,
    _service_token_env,
    _tunnel_cmd,
    DEFAULT_HOST,
    DEFAULT_DBNAME,
    DEFAULT_LOCAL_PORT,
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
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `$env:PYTHONIOENCODING='utf-8'; & C:/ProgramData/anaconda3/python.exe -m pytest tests/test_quantdb.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'topquant_ksk.db.quantdb'`

- [ ] **Step 3: 최소 구현 작성**

`src/topquant_ksk/db/quantdb.py`:

```python
import os
import time
import subprocess
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text

from .tunnel import find_cloudflared

DEFAULT_HOST = "shquantdb.alphawaves.vip"
DEFAULT_DBNAME = "quantdb"
DEFAULT_LOCAL_PORT = 15432


def _make_dsn(db_user, db_password, local_port, dbname):
    return f"postgresql://{db_user}:{quote_plus(db_password)}@127.0.0.1:{local_port}/{dbname}"


def _service_token_env(cf_client_id, cf_client_secret):
    if cf_client_id and cf_client_secret:
        return {
            "TUNNEL_SERVICE_TOKEN_ID": cf_client_id,
            "TUNNEL_SERVICE_TOKEN_SECRET": cf_client_secret,
        }
    return {}


def _tunnel_cmd(cloudflared_exe, hostname, local_port):
    return [cloudflared_exe, "access", "tcp", "--hostname", hostname, "--url", f"127.0.0.1:{local_port}"]
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `$env:PYTHONIOENCODING='utf-8'; & C:/ProgramData/anaconda3/python.exe -m pytest tests/test_quantdb.py -v`
Expected: PASS (TestMakeDsn, TestServiceTokenEnv, TestTunnelCmd, test_defaults)

- [ ] **Step 5: 커밋**

```
git add src/topquant_ksk/db/quantdb.py tests/test_quantdb.py
git commit -m "feat(db): QuantDB pure config helpers (dsn, service-token env, tunnel cmd)"
```

---

### Task 2: QuantDB 클래스 + read_sql + engine

**Files:**
- Modify: `src/topquant_ksk/db/quantdb.py`
- Test: `tests/test_quantdb.py`

**Interfaces:**
- Consumes: `_make_dsn`, `text` (Task 1)
- Produces:
  - `class QuantDB(db_user, db_password, *, hostname=DEFAULT_HOST, dbname=DEFAULT_DBNAME, local_port=DEFAULT_LOCAL_PORT, cf_client_id=None, cf_client_secret=None, tunnel_wait=4.0, connect_timeout=20)`
  - `QuantDB.engine -> sqlalchemy.Engine` (property; 컨텍스트 밖이면 `RuntimeError`)
  - `QuantDB.read_sql(sql: str, params: dict | None = None) -> pandas.DataFrame`
  - `cf_client_id`/`cf_client_secret` 가 `None` 이면 `os.environ` 의 `CF_ACCESS_CLIENT_ID`/`CF_ACCESS_CLIENT_SECRET` 사용
  - 내부 상태: `_engine`(기본 None), `_tunnel`(기본 None)

- [ ] **Step 1: 실패하는 테스트 작성** — `tests/test_quantdb.py` 에 추가

```python
import os
import pytest
from topquant_ksk.db.quantdb import QuantDB


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
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `$env:PYTHONIOENCODING='utf-8'; & C:/ProgramData/anaconda3/python.exe -m pytest tests/test_quantdb.py -v`
Expected: FAIL — `ImportError: cannot import name 'QuantDB'` (또는 AttributeError)

- [ ] **Step 3: 최소 구현 작성** — `quantdb.py` 끝에 추가

```python
class QuantDB:
    def __init__(self, db_user, db_password, *, hostname=DEFAULT_HOST, dbname=DEFAULT_DBNAME,
                 local_port=DEFAULT_LOCAL_PORT, cf_client_id=None, cf_client_secret=None,
                 tunnel_wait=4.0, connect_timeout=20):
        if not db_user or not db_password:
            raise ValueError("db_user 와 db_password 는 필수입니다 (None/빈 문자열 불가).")
        self.db_user = db_user
        self.db_password = db_password
        self.hostname = hostname
        self.dbname = dbname
        self.local_port = local_port
        self.cf_client_id = cf_client_id if cf_client_id is not None else os.environ.get("CF_ACCESS_CLIENT_ID")
        self.cf_client_secret = (
            cf_client_secret if cf_client_secret is not None else os.environ.get("CF_ACCESS_CLIENT_SECRET")
        )
        self.tunnel_wait = tunnel_wait
        self.connect_timeout = connect_timeout
        self._engine = None
        self._tunnel = None

    @property
    def engine(self):
        if self._engine is None:
            raise RuntimeError("QuantDB 는 `with QuantDB(...) as db:` 컨텍스트 안에서 사용하세요.")
        return self._engine

    def read_sql(self, sql, params=None):
        with self.engine.connect() as conn:
            res = conn.execute(text(sql), params or {})
            return pd.DataFrame(res.fetchall(), columns=list(res.keys()))
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `$env:PYTHONIOENCODING='utf-8'; & C:/ProgramData/anaconda3/python.exe -m pytest tests/test_quantdb.py -v`
Expected: PASS (모든 Task 1 + Task 2 테스트)

- [ ] **Step 5: 커밋**

```
git add src/topquant_ksk/db/quantdb.py tests/test_quantdb.py
git commit -m "feat(db): QuantDB class with read_sql and engine property"
```

---

### Task 3: 컨텍스트매니저 (터널 + 엔진 lifecycle)

**Files:**
- Modify: `src/topquant_ksk/db/quantdb.py`
- Test: `tests/test_quantdb.py`

**Interfaces:**
- Consumes: `find_cloudflared`, `_tunnel_cmd`, `_service_token_env`, `_make_dsn`, `subprocess`, `time`, `create_engine`, `text` (Task 1·2)
- Produces:
  - `QuantDB.__enter__() -> QuantDB` (터널 spawn + 검증 엔진 생성, `_engine`/`_tunnel` 설정)
  - `QuantDB.__exit__(exc_type, exc, tb) -> False` (엔진 dispose + 터널 kill)
  - `QuantDB._start_tunnel()`, `QuantDB._kill_tunnel()`, `QuantDB._create_verified_engine(dsn, max_retries=3, retry_delay=1)`

- [ ] **Step 1: 실패하는 테스트 작성** — `tests/test_quantdb.py` 에 추가

```python
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
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `$env:PYTHONIOENCODING='utf-8'; & C:/ProgramData/anaconda3/python.exe -m pytest tests/test_quantdb.py -v`
Expected: FAIL — `AttributeError: ... '_start_tunnel'` / context manager protocol 없음

- [ ] **Step 3: 최소 구현 작성** — `QuantDB` 클래스에 메서드 추가

```python
    def __enter__(self):
        self._tunnel = self._start_tunnel()
        dsn = _make_dsn(self.db_user, self.db_password, self.local_port, self.dbname)
        self._engine = self._create_verified_engine(dsn)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
        self._kill_tunnel()
        return False

    def _start_tunnel(self):
        exe = find_cloudflared()
        if exe is None:
            raise RuntimeError("cloudflared 실행파일을 찾을 수 없습니다.")
        env = dict(os.environ, **_service_token_env(self.cf_client_id, self.cf_client_secret))
        proc = subprocess.Popen(
            _tunnel_cmd(exe, self.hostname, self.local_port),
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env,
        )
        time.sleep(self.tunnel_wait)
        return proc

    def _kill_tunnel(self):
        if self._tunnel is None:
            return
        subprocess.run(
            ["taskkill", "/F", "/T", "/PID", str(self._tunnel.pid)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        self._tunnel = None

    def _create_verified_engine(self, dsn, max_retries=3, retry_delay=1):
        last_err = None
        for attempt in range(max_retries):
            engine = create_engine(
                dsn, pool_pre_ping=True, connect_args={"connect_timeout": self.connect_timeout}
            )
            try:
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                return engine
            except Exception as e:
                engine.dispose()
                last_err = e
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        raise last_err
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `$env:PYTHONIOENCODING='utf-8'; & C:/ProgramData/anaconda3/python.exe -m pytest tests/test_quantdb.py -v`
Expected: PASS (Task 1·2·3 모든 테스트)

- [ ] **Step 5: 커밋**

```
git add src/topquant_ksk/db/quantdb.py tests/test_quantdb.py
git commit -m "feat(db): QuantDB context manager (tunnel + verified engine lifecycle)"
```

---

### Task 4: db 패키지에서 QuantDB export

**Files:**
- Modify: `src/topquant_ksk/db/__init__.py`
- Test: `tests/test_quantdb.py`

**Interfaces:**
- Produces: `from topquant_ksk.db import QuantDB` 가 동작

- [ ] **Step 1: 실패하는 테스트 작성** — `tests/test_quantdb.py` 상단 근처에 추가

```python
def test_quantdb_exported_from_db_package():
    from topquant_ksk.db import QuantDB as ExportedQuantDB
    from topquant_ksk.db.quantdb import QuantDB as DirectQuantDB
    assert ExportedQuantDB is DirectQuantDB
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `$env:PYTHONIOENCODING='utf-8'; & C:/ProgramData/anaconda3/python.exe -m pytest tests/test_quantdb.py::test_quantdb_exported_from_db_package -v`
Expected: FAIL — `ImportError: cannot import name 'QuantDB' from 'topquant_ksk.db'`

- [ ] **Step 3: 최소 구현 작성** — `src/topquant_ksk/db/__init__.py` 끝에 한 줄 추가

```python
from .quantdb import QuantDB
```

(최종 `__init__.py`:)

```python
from . import tunnel
from . import upload
from . import tools
from . import download
from . import telegram
from .connection import DBConnection
from .quantdb import QuantDB
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `$env:PYTHONIOENCODING='utf-8'; & C:/ProgramData/anaconda3/python.exe -m pytest tests/test_quantdb.py -v`
Expected: PASS

- [ ] **Step 5: 커밋**

```
git add src/topquant_ksk/db/__init__.py tests/test_quantdb.py
git commit -m "feat(db): export QuantDB from db package"
```

---

### Task 5a: 예시 스크립트 전체 재작성 (QuantDB + env 시크릿) + merge_asof 단위테스트

**Files:**
- Modify: `series_kodex_spy_krw_tunnel.py` (전체 재작성 — 한 번에)
- Test: `tests/test_series_kodex_spy_krw.py`

> **codex finding 1:** build() 만 교체하면 모듈 top-level 의 import-시점 `raise RuntimeError`(CF 토큰 가드)가 남아, importlib 로 로드하는 테스트가 import 단계에서 실패한다. 따라서 Step 3 에서 스크립트 **전체**를 한 번에 재작성해 import-clean 으로 만든다 (반쪽 재작성 중간상태 금지).

**Interfaces:**
- Consumes: 임의의 `db` 객체로서 `db.read_sql(sql, params) -> pandas.DataFrame` 만 요구 (QuantDB 또는 fake)
- Produces: `build(db, since="2003-01-01") -> pandas.DataFrame` (컬럼: `date, kodex_close_krw, spy_prev_close_usd, usdkrw, spy_prev_close_krw`)

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/test_series_kodex_spy_krw.py`:

```python
import importlib.util
import os
import pandas as pd

# 스크립트는 패키지가 아니라 repo 루트의 단일 파일 → 경로로 직접 로드
_SCRIPT = os.path.join(os.path.dirname(__file__), "..", "series_kodex_spy_krw_tunnel.py")
_spec = importlib.util.spec_from_file_location("series_kodex_spy_krw_tunnel", _SCRIPT)
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)


class _FakeDB:
    """sql 내용으로 어떤 시계열을 요청하는지 판별해 캔드 프레임 반환."""

    def __init__(self):
        self.calls = []

    def read_sql(self, sql, params=None):
        self.calls.append((sql, params))
        if "069500.KS" in sql:
            return pd.DataFrame({
                "date": ["2003-01-02", "2003-01-03"],
                "kodex_close_krw": [100.0, 110.0],
            })
        if "ticker='SPY'" in sql:
            return pd.DataFrame({
                "date": ["2003-01-01", "2003-01-02"],
                "spy_prev_close_usd": [50.0, 55.0],
            })
        if "ai_ready.fx" in sql:
            return pd.DataFrame({
                "date": ["2003-01-02", "2003-01-03"],
                "usdkrw": [1200.0, 1210.0],
            })
        raise AssertionError(f"예상치 못한 쿼리: {sql}")


def test_build_merges_and_derives_krw():
    db = _FakeDB()
    out = mod.build(db, since="2003-01-01")

    assert list(out.columns) == [
        "date", "kodex_close_krw", "spy_prev_close_usd", "usdkrw", "spy_prev_close_krw"
    ]
    # 첫 read_sql(KODEX) 에 since 바인딩 전달
    assert db.calls[0][1] == {"since": "2003-01-01"}

    row = out[out["date"] == pd.Timestamp("2003-01-03")].iloc[0]
    # SPY 전일종가 = strictly < 01-03 의 마지막 = 01-02 의 55
    assert row["spy_prev_close_usd"] == 55.0
    # 당일 환율 = <= 01-03 = 01-03 의 1210
    assert row["usdkrw"] == 1210.0
    assert row["spy_prev_close_krw"] == 55.0 * 1210.0
    assert row["kodex_close_krw"] == 110.0


def test_build_spy_is_strictly_previous():
    db = _FakeDB()
    out = mod.build(db, since="2003-01-01")
    row = out[out["date"] == pd.Timestamp("2003-01-02")].iloc[0]
    # 01-02 의 SPY 전일종가 = strictly < 01-02 = 01-01 의 50 (01-02 자기 자신 아님)
    assert row["spy_prev_close_usd"] == 50.0
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `$env:PYTHONIOENCODING='utf-8'; & C:/ProgramData/anaconda3/python.exe -m pytest tests/test_series_kodex_spy_krw.py -v`
Expected: FAIL/ERROR — importlib 로 구 스크립트를 exec 하면 top-level 의 `raise RuntimeError("CF Access 서비스토큰 없음 ...")` 가 먼저 터지거나(CF env 없을 때), 토큰이 set 돼 있으면 구 `build(engine)` 가 `engine.connect()` 를 호출해 `_FakeDB` 와 불일치(AttributeError). 어느 쪽이든 수집/실행 단계 실패.

- [ ] **Step 3: 스크립트 전체 재작성** — `series_kodex_spy_krw_tunnel.py` 전체를 아래로 교체

```python
"""KODEX 200 당일종가 + SPY 전일종가×당일 USDKRW(KRW 환산) 시계열, 2003년~.

원격 AI_Quant quantdb(ai_ready.etf_timeseries / ai_ready.fx)에 접속.
DB 접근은 topquant_ksk.db.QuantDB 가 담당 (cloudflared 터널 + CF Access 서비스토큰).
시크릿(DB_USER/DB_PASSWORD/CF_ACCESS_CLIENT_ID/CF_ACCESS_CLIENT_SECRET)은 .env 에서 주입.

컬럼:
  date               : KR 거래일 (KODEX 200 거래일 기준)
  kodex_close_krw    : KODEX 200(069500.KS) 당일 종가 (adj_close_pr, KRW)
  spy_prev_close_usd : SPY 전일 종가 = 해당 KR일자보다 이전(<date) 마지막 SPY 종가 (USD)
  usdkrw             : 당일 SMBS 서울 15:30 종가 (KRW/USD; 없으면 직전 거래일)
  spy_prev_close_krw : spy_prev_close_usd × usdkrw
"""
import argparse
import os

import pandas as pd

from topquant_ksk.db import QuantDB

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))


def _require_env(name):
    val = os.environ.get(name)
    if not val:
        raise RuntimeError(
            f"환경변수 {name} 가 없습니다. repo 루트 .env 에 설정하거나 셸 환경변수로 주입하세요. "
            f"(.env 사용 시 python-dotenv 필요: pip install topquant-ksk[db])"
        )
    return val


def build(db, since="2003-01-01"):
    def q(sql, params=None):
        df = db.read_sql(sql, params)
        df["date"] = pd.to_datetime(df["date"])
        return df

    kodex = q("SELECT date, adj_close_pr AS kodex_close_krw FROM ai_ready.etf_timeseries "
              "WHERE ticker='069500.KS' AND date >= :since ORDER BY date", {"since": since})
    spy = q("SELECT date, adj_close_pr AS spy_prev_close_usd FROM ai_ready.etf_timeseries "
            "WHERE ticker='SPY' ORDER BY date")
    fx = q("SELECT date, usdkrw_smbs_close AS usdkrw FROM ai_ready.fx "
           "WHERE usdkrw_smbs_close IS NOT NULL ORDER BY date")

    # KR 거래일(KODEX)에 정렬. SPY는 strictly 이전(<date) 마지막 종가 = 전일종가.
    df = pd.merge_asof(kodex, spy, on="date", direction="backward", allow_exact_matches=False)
    # 당일 SMBS 환율(없으면 직전 거래일).
    df = pd.merge_asof(df, fx, on="date", direction="backward", allow_exact_matches=True)
    df["spy_prev_close_krw"] = df["spy_prev_close_usd"] * df["usdkrw"]
    return df[["date", "kodex_close_krw", "spy_prev_close_usd", "usdkrw", "spy_prev_close_krw"]]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2003-01-01")
    ap.add_argument("--out", default=None, help="CSV 경로 (없으면 head/tail 미리보기)")
    a = ap.parse_args()

    with QuantDB(db_user=_require_env("DB_USER"), db_password=_require_env("DB_PASSWORD")) as db:
        df = build(db, a.since)

    if a.out:
        df.to_csv(a.out, index=False, encoding="utf-8-sig")
        print("wrote %d rows -> %s (%s..%s)"
              % (len(df), a.out, df.date.min().date(), df.date.max().date()))
    else:
        with pd.option_context("display.width", 160):
            print(df.head().to_string(index=False))
            print("...")
            print(df.tail().to_string(index=False))
        print("rows=%d span=%s..%s" % (len(df), df.date.min().date(), df.date.max().date()))


if __name__ == "__main__":
    main()
```

제거되는 것: `shutil`/`subprocess`/`time`/`quote_plus` import, `HOSTNAME`/`DBNAME`/`LOCAL_PORT`/`DB_USER`/`DB_PASSWORD` 상수, `CF_ID`/`CF_SEC` + import-시점 RuntimeError 가드, `_find_cloudflared`/`start_tunnel`/`kill_tunnel`, `create_engine`/DSN 조립. (import 는 전부 파일 맨 위 — 프로젝트 규칙 준수.)

- [ ] **Step 4: 테스트 통과 확인**

Run: `$env:PYTHONIOENCODING='utf-8'; & C:/ProgramData/anaconda3/python.exe -m pytest tests/test_series_kodex_spy_krw.py -v`
Expected: PASS (importlib exec 가 import-clean 이므로 통과; `main()` 은 호출 안 되어 `_require_env`/env 불필요)

- [ ] **Step 5: 커밋**

```
git add series_kodex_spy_krw_tunnel.py tests/test_series_kodex_spy_krw.py
git commit -m "refactor: series_kodex_spy_krw_tunnel.py uses QuantDB (secrets via .env), unit-test merge_asof"
```

---

### Task 5b: `.env.example` + 통합 검증

**Files:**
- Create: `.env.example`

(스크립트 전체 재작성은 Task 5a Step 3 에서 완료됨. 여기선 시크릿 템플릿 생성과 실제 DB 통합 검증만 수행.)

- [ ] **Step 1: `.env.example` 작성** (DB_USER 도 값 비움 — 실제 값은 `.env` 에만; codex finding 4)

```
# series_kodex_spy_krw_tunnel.py 실행에 필요한 시크릿. 복사해서 .env 로 저장하고 값 채우기.
# .env 는 gitignore 됨 (절대 commit 금지). 아래는 변수명만 — 실제 값 넣지 말 것.
DB_USER=
DB_PASSWORD=
CF_ACCESS_CLIENT_ID=
CF_ACCESS_CLIENT_SECRET=
```

- [ ] **Step 2: 단위테스트 회귀 확인**

Run: `$env:PYTHONIOENCODING='utf-8'; & C:/ProgramData/anaconda3/python.exe -m pytest tests/test_series_kodex_spy_krw.py tests/test_quantdb.py -v`
Expected: PASS (Task 5a 에서 통과한 테스트 그대로 유지)

- [ ] **Step 3: 수동 통합 검증** (실제 DB — 자동 테스트 불가)

사전: repo 루트에 `.env` 생성(`.env.example` 복사 후 실제 값), cloudflared 설치됨.

Run:
```
$env:PYTHONIOENCODING='utf-8'; & C:/ProgramData/anaconda3/python.exe series_kodex_spy_krw_tunnel.py --since 2024-01-01
```
Expected: head/tail 미리보기 출력 + `rows=... span=2024-...` 한 줄. 에러 없이 종료(터널 자동 정리).

- [ ] **Step 4: 커밋**

```
git add .env.example
git commit -m "chore: add .env.example for series_kodex_spy_krw_tunnel secrets"
```

---

### Task 6: 전체 테스트 스위트 회귀 + 문서 갱신

**Files:**
- Modify: `docs/db/connection.md` (QuantDB 섹션 추가)

- [ ] **Step 1: 전체 테스트 통과 확인** (기존 테스트 안 깨졌는지)

Run: `$env:PYTHONIOENCODING='utf-8'; & C:/ProgramData/anaconda3/python.exe -m pytest -v`
Expected: PASS (test_quantdb, test_series_kodex_spy_krw, test_tools, test_risk_return_metrics 모두)

- [ ] **Step 2: 문서 추가** — `docs/db/connection.md` 끝에 QuantDB 섹션 추가

````markdown
## QuantDB (quantdb 배포본 / ai_ready 스키마)

`quantdb` 배포본(`ai_ready` 스키마, Cloudflare Access 서비스토큰)에 raw SQL 로 접속하는 컨텍스트매니저. 기존 `DBConnection`/fetcher 와 별개의 forward 경로입니다 (ADR-0001).

```python
import os
from topquant_ksk.db import QuantDB

with QuantDB(db_user=os.environ["DB_USER"], db_password=os.environ["DB_PASSWORD"]) as db:
    df = db.read_sql(
        "SELECT date, adj_close_pr FROM ai_ready.etf_timeseries "
        "WHERE ticker = :t AND date >= :since ORDER BY date",
        {"t": "069500.KS", "since": "2024-01-01"},
    )
```

- 터널을 1회 열고 컨텍스트 안에서 `read_sql` 을 여러 번 호출 → `__exit__` 에서 터널·엔진 자동 정리.
- CF 서비스토큰은 기본적으로 env `CF_ACCESS_CLIENT_ID`/`CF_ACCESS_CLIENT_SECRET` 에서 읽음 (생성자 인자로 override 가능). 시크릿은 코드에 하드코딩하지 않습니다.
- `db.engine` 으로 SQLAlchemy 엔진에 직접 접근 가능 (비-SELECT/고급 용도).

| 파라미터 | 기본값 | 설명 |
|----------|--------|------|
| `db_user`, `db_password` | - | 필수 |
| `hostname` | `shquantdb.alphawaves.vip` | 터널 public hostname |
| `dbname` | `quantdb` | 데이터베이스명 |
| `local_port` | `15432` | 로컬 터널 포트 |
| `cf_client_id` / `cf_client_secret` | env | CF Access 서비스토큰 (없으면 서비스토큰 없이 터널) |
| `tunnel_wait` | `4.0` | 터널 기동 대기(초) |
| `connect_timeout` | `20` | 접속 타임아웃(초) |
````

- [ ] **Step 3: 커밋**

```
git add docs/db/connection.md
git commit -m "docs(db): document QuantDB forward path"
```

---

## Self-Review

**1. Spec coverage** (grill 결정 8개 대응):
- 멀티타깃 additive / quant_data 불변 → Task 1~4 가 신규 파일·export 만 추가, 기존 모듈 0 변경 (Global Constraints).
- 새 클래스 QuantDB + 컨텍스트매니저 → Task 2·3.
- 기본값 quantdb/shquantdb/15432 → Task 1 상수 + Task 2 `test_defaults_target_quantdb`.
- CF 토큰 env 자동 + override, plain 터널 fallback → Task 2 `test_cf_token_*`, Task 3 `test_start_tunnel_*`.
- read_sql + .engine, text() 바인딩, pandas → Task 2.
- 시크릿 .env → Task 5a (`_require_env` + main 이 env 에서 주입) + Task 5b (`.env.example`) + Task 0 (`python-dotenv` 선언), Global Constraints.
- lib=raw I/O, merge_asof 스크립트 잔존 → Task 5a (build 가 스크립트에 남고 db 만 주입; fake db 로 단위테스트).
- 테스트 import 체인이 `[db]` 의존 → Task 0 (CI 에 `[db]` 설치).
- ADR/CONTEXT 참조 → header + Task 6 문서.

**2. Placeholder scan:** 모든 step 에 실제 코드/명령/기대출력 포함. TBD/TODO 없음.

**3. Type consistency:** `read_sql(sql, params=None) -> DataFrame`, `build(db, since)`, `_make_dsn(user, password, local_port, dbname)`, `_tunnel_cmd(exe, hostname, local_port)`, `_service_token_env(id, secret)` — Task 간 시그니처 일치 확인.

**4. codex gate 반영 (2026-06-18):**
- [finding 1, MAJOR] importlib 테스트가 import-시점 RuntimeError 로 깨짐 → Task 5a 를 build()-only 가 아닌 **전체 재작성**으로 변경 (반쪽 상태 제거).
- [finding 2, MAJOR] `python-dotenv` 미선언 → Task 0 에서 `[db]` 에 추가 + Task 5a 스크립트에 `_require_env` 로 명확한 오류 메시지.
- [finding 3, MINOR] 테스트 env 오염 → Task 3 `test_start_tunnel_no_token_when_absent` 에 `monkeypatch.delenv` 추가.
- [finding 4, NIT] `.env.example` 의 실제 DB_USER 값 → Task 5b 에서 `DB_USER=` 로 비움.
- [self, BLOCKER] 신규 테스트가 `topquant_ksk.db` import 시 `[db]` 미설치면 CI 실패 (codex 미발견) → Task 0 에서 CI 를 `.[plot,db]` 로.

비고:
- `local_host`(localhost 직결) 은 이번 범위 제외 (YAGNI; quantdb 는 원격 전용). 필요 시 후속.
- `read_sql` 의 `res.keys()` 는 SQLAlchemy `CursorResult.keys()` (RMKeyView) → `list()` 로 컬럼명 변환. 빈 결과셋이면 빈 DataFrame.
- `upload.py` 의 `quote_plus` 누락 버그는 별개 follow-up note (`docs/follow-up-note/pending-review/2026-06-18-upload-uri-missing-quote-plus.md`) — 이 계획 범위 밖.
- Task 0 비고: ubuntu CI 에서 `import xlwings` 실패 시 대안은 `db/__init__.py` lazy import (기존 모듈 변경이라 범위 밖) — 우선 `[db]` 설치로 진행.
