import os
import subprocess
import time
import warnings
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text

from .tunnel import find_cloudflared

try:
    from dotenv import dotenv_values, find_dotenv
except ImportError:  # python-dotenv 미설치 (선택적 의존성 [db])
    dotenv_values = None
    find_dotenv = None

DEFAULT_DBNAME = "quantdb"
DEFAULT_HOSTNAME = "shquantdb.alphawaves.vip"
DEFAULT_LOCAL_PORT = 15432


def load_env(path=None, override=True, warn_conflicts=True):
    """`.env` 파일을 `os.environ` 으로 로드한다.

    - override=True (기본): `.env` 값이 기존 OS 환경변수를 덮어쓴다 (.env 가 진실의 한 지점).
    - warn_conflicts=True: OS 환경변수와 `.env` 값이 **다를 때** 경고를 출력한다.
      경고에는 키 이름만 표시하고 값은 절대 출력하지 않는다 (시크릿 로그 유출 방지).
    - python-dotenv 미설치 시 경고 후 아무것도 하지 않는다.

    반환: 실제로 적용된 {키: 값} 딕셔너리.
    """
    if dotenv_values is None:
        warnings.warn("python-dotenv 미설치 — .env 로드 건너뜀 (pip install topquant-ksk[db])")
        return {}
    if path is None:
        path = find_dotenv(usecwd=True) or ".env"
    values = dotenv_values(path)
    applied = {}
    for key, val in values.items():
        if val is None:
            continue
        existing = os.environ.get(key)
        if existing is not None and existing != val:
            if warn_conflicts:
                winner = ".env" if override else "OS 환경변수"
                warnings.warn(f"[load_env] '{key}' 충돌: OS 환경변수와 .env 값이 다름 → {winner} 값 사용.")
            if not override:
                continue
        os.environ[key] = val
        applied[key] = val
    return applied


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


class QuantDB:
    """quantdb 배포본(ai_ready 스키마)에 cloudflared 터널로 접속하는 컨텍스트매니저.

    설정은 전부 인자로 직접 받는다 (os.environ 을 내부에서 읽지 않음).
    필수: db_user/db_password. dbname/hostname/local_port 등은 기본값이 있고 override 가능.
    `.env` 값을 쓰려면 호출자가 `load_env()` 로 로드한 뒤 직접 인자로 넘긴다:

        load_env()
        QuantDB(db_user=os.environ.get("DB_USER"), db_password=os.environ.get("DB_PASSWORD"))
    """

    def __init__(self, db_user, db_password, dbname=DEFAULT_DBNAME, hostname=DEFAULT_HOSTNAME, *,
                 local_port=DEFAULT_LOCAL_PORT, cf_client_id=None, cf_client_secret=None,
                 cloudflared_bin=None, tunnel_wait=4.0, connect_timeout=20):
        # 클래스는 credential/설정을 직접 인자로 받는다 (os.environ 을 내부에서 읽지 않음).
        # 실제 필수 입력은 credential(db_user/db_password). dbname/hostname 은 기본값이 있고 override 가능.
        missing = [
            name for name, val in (
                ("db_user", db_user), ("db_password", db_password),
                ("dbname", dbname), ("hostname", hostname),
            ) if not val
        ]
        if missing:
            raise ValueError(f"QuantDB: 필수 credential/설정 누락 또는 빈값: {missing}")
        self.db_user = db_user
        self.db_password = db_password
        self.dbname = dbname
        self.hostname = hostname
        self.local_port = int(local_port)
        self.cf_client_id = cf_client_id
        self.cf_client_secret = cf_client_secret
        self.cloudflared_bin = cloudflared_bin
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

    def list_tables(self, schema=None, verbose=False):
        """현재 계정이 실제 SELECT 가능한 테이블/뷰/matview/foreign 목록 → pandas.DataFrame.

        컬럼: schema, name, type(table/view/matview/foreign), columns(컬럼명 전부, 콤마 구분).
        - schema USAGE + object SELECT 를 모두 가진 객체만 (실제 접근가능).
        - system 스키마(pg_*, information_schema, timescaledb 내부) 제외.
        - schema: 지정 시 해당 스키마만. None 이면 접근가능한 전체.
        - verbose=True 면 스키마별 개수 요약도 print.
        """
        sql = """
            SELECT n.nspname AS schema,
                   c.relname AS name,
                   CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'table'
                                  WHEN 'v' THEN 'view'  WHEN 'm' THEN 'matview'
                                  WHEN 'f' THEN 'foreign' END AS type,
                   (SELECT string_agg(a.attname, ', ' ORDER BY a.attnum) FROM pg_attribute a
                    WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped) AS columns
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
              AND n.nspname !~ '^pg_'
              AND n.nspname !~ '^_timescaledb'
              AND n.nspname NOT IN ('information_schema', 'timescaledb_information', 'timescaledb_experimental')
              AND has_schema_privilege(n.oid, 'USAGE')
              AND has_table_privilege(c.oid, 'SELECT')
              AND (:schema IS NULL OR n.nspname = :schema)
            ORDER BY n.nspname, c.relname
        """
        df = self.read_sql(sql, {"schema": schema})
        if verbose:
            if len(df):
                print(f"접근가능 객체 {len(df)}개:")
                for sch, cnt in df.groupby("schema").size().items():
                    print(f"  {sch}: {cnt}개")
            else:
                print("접근가능한 객체 없음.")
        return df

    def __enter__(self):
        self._tunnel = self._start_tunnel()
        try:
            dsn = _make_dsn(self.db_user, self.db_password, self.local_port, self.dbname)
            self._engine = self._create_verified_engine(dsn)
        except Exception:
            # 엔진 생성 실패 시 __exit__ 가 호출되지 않으므로 여기서 터널을 정리한다.
            self._kill_tunnel()
            raise
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
        self._kill_tunnel()
        return False

    def _start_tunnel(self):
        exe = self.cloudflared_bin or find_cloudflared()
        if not exe:
            raise RuntimeError(
                "cloudflared 실행파일을 찾을 수 없습니다 (.env 의 CLOUDFLARED_BIN 설정 또는 PATH 확인)."
            )
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
