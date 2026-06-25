import os
import subprocess
import time
import warnings
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text

from .tunnel import find_cloudflared

DEFAULT_DBNAME = "quantdb"
DEFAULT_HOSTNAME = "shquantdb.alphawaves.vip"
DEFAULT_LOCAL_PORT = 15432
DEFAULT_POSTGRES_PORT = 5432  # local_host=True 일 때 직결할 로컬 Postgres 포트


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
    local_host=True 면 cloudflared 터널 없이 로컬 Postgres(127.0.0.1:5432) 직결 (quantdb PC 용, 기본 False).
    `.env` 를 쓰려면 호출자가 직접 읽어 인자로 넘긴다 (python-dotenv 예):

        from dotenv import dotenv_values
        cfg = dotenv_values()
        QuantDB(db_user=cfg["DB_USER"], db_password=cfg["DB_PASSWORD"])
    """

    def __init__(self, db_user, db_password, dbname=DEFAULT_DBNAME, hostname=DEFAULT_HOSTNAME, *,
                 local_host=False, local_port=DEFAULT_LOCAL_PORT, cf_client_id=None, cf_client_secret=None,
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
        self.local_host = bool(local_host)
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

    def read_sql(self, sql, params=None, verbose=True):
        """SQL 조회 → pandas.DataFrame.

        - params: :name 바인딩 딕셔너리 (injection 안전).
        - verbose=True(기본): 결과를 print (셀 잘림 없음). False 면 조용히 DataFrame 만 반환.
        """
        with self.engine.connect() as conn:
            res = conn.execute(text(sql), params or {})
            df = pd.DataFrame(res.fetchall(), columns=list(res.keys()))
        if verbose:
            with pd.option_context("display.max_colwidth", None, "display.max_columns", None,
                                   "display.width", 1000):
                print(df)
        return df

    def list_tables(self, schema=None, verbose=True):
        """현재 계정이 실제 SELECT 가능한 테이블/뷰/matview/foreign 목록 → pandas.DataFrame.

        컬럼: schema, name, type(table/view/matview/foreign), columns(컬럼명 전부, 콤마 구분).
        - schema USAGE + object SELECT 를 모두 가진 객체만 (실제 접근가능).
        - system 스키마(pg_*, information_schema, timescaledb 내부) 제외.
        - schema: 지정 시 해당 스키마만. None 이면 접근가능한 전체.
        - verbose=True(기본): 객체별로 이름 + 컬럼 전부를 잘리지 않게 print. False 면 조용히 DataFrame 만 반환.
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
        df = self.read_sql(sql, {"schema": schema}, verbose=False)
        if verbose:
            if len(df):
                print(f"접근가능 객체 {len(df)}개:")
                for _, r in df.iterrows():
                    print(f"  [{r['schema']}.{r['name']}] ({r['type']})")
                    print(f"      {r['columns']}")
            else:
                print("접근가능한 객체 없음.")
        return df

    def fetch_timeseries(self, relation, fields=None, tickers=None, start=None, end=None, verbose=True):
        """ai_ready timeseries 패널 객체(table/view/matview)를 wide pivot DataFrame 으로 가져온다.

        반환: index=시간(date/ts 자동), columns=MultiIndex(item=값필드, ticker, name, isin)
        (식별자는 객체에 존재하는 것만; fx_daily 는 iso_code/currency_name).

        - relation: ai_ready 객체명. relkind 무관 — table/view/matview/foreign 모두 가능 (pg_catalog 컬럼감지).
        - fields: 값 컬럼 리스트 (None 이면 numeric 측정값 자동 — *id/식별자 제외).
        - tickers: 엔티티(ticker, fx 는 iso_code) 필터 리스트. None 이면 전체.
        - start/end: 시간 범위 (시간 컬럼 기준).
        - verbose=True: 미필터 경고 + 결과 요약 print.
        """
        full = relation if "." in relation else f"ai_ready.{relation}"
        schema, relname = full.split(".", 1)

        # pg_catalog 로 컬럼 + numeric 여부 감지 (information_schema 는 matview 누락).
        cols = self.read_sql(
            """
            SELECT a.attname AS column_name, t.typcategory AS typcat
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_type t ON t.oid = a.atttypid
            WHERE n.nspname = :s AND c.relname = :t AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
            """,
            {"s": schema, "t": relname}, verbose=False)
        if cols.empty:
            raise ValueError(f"{full}: 컬럼이 없습니다 (테이블/뷰/matview 없음 또는 접근 불가).")
        colnames = list(cols["column_name"])
        numeric_cols = set(cols.loc[cols["typcat"] == "N", "column_name"])

        time_col = "ts" if "ts" in colnames else ("date" if "date" in colnames else None)
        if time_col is None:
            raise ValueError(f"{full}: 시간 컬럼(ts/date) 없음 — timeseries 패널 뷰가 아닙니다. read_sql 을 쓰세요.")
        identity = [c for c in ("ticker", "name", "isin") if c in colnames]
        if not identity:                      # ticker 없는 뷰(fx_daily 등)는 iso_code 그룹
            identity = [c for c in ("iso_code", "currency_name") if c in colnames]
        if not identity:
            raise ValueError(f"{full}: 식별자 컬럼(ticker/iso_code 등) 없음.")
        entity = identity[0]

        if fields is None:
            value_cols = [c for c in colnames
                          if c in numeric_cols and not c.endswith("id")
                          and c not in identity and c != time_col]
        else:
            value_cols = [fields] if isinstance(fields, str) else list(fields)
        if not value_cols:
            raise ValueError(f"{full}: 값 컬럼이 없습니다 (fields 를 지정하세요).")

        conditions, params = [], {}
        if tickers is not None:
            params["tickers"] = [tickers] if isinstance(tickers, str) else list(tickers)
            conditions.append(f"{entity} = ANY(:tickers)")
        if start is not None:
            params["start"] = start
            conditions.append(f"{time_col} >= :start")
        if end is not None:
            params["end"] = end
            conditions.append(f"{time_col} <= :end")
        if not conditions:
            warnings.warn(f"fetch_timeseries({full}): tickers/기간 미지정 — 전체 fetch (대용량 위험).")

        select = ", ".join([time_col] + identity + value_cols)
        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        long = self.read_sql(f"SELECT {select} FROM {full}{where} ORDER BY {time_col}", params, verbose=False)
        if long.empty:
            if verbose:
                print(f"fetch_timeseries({full}): 0행 (조건에 맞는 데이터 없음).")
            return long

        long[time_col] = pd.to_datetime(long[time_col])
        wide = long.pivot(index=time_col, columns=identity, values=value_cols)
        wide.columns = wide.columns.set_names(["item"] + identity)
        if verbose:
            print(f"fetch_timeseries({full}): {wide.shape[0]:,}행 x {wide.shape[1]:,}열 "
                  f"[{wide.index.min()} ~ {wide.index.max()}], fields={value_cols}")
        return wide

    def __enter__(self):
        if not self.local_host:                       # local_host 면 터널 없이 로컬 Postgres 직결
            self._tunnel = self._start_tunnel()
        try:
            port = DEFAULT_POSTGRES_PORT if self.local_host else self.local_port
            dsn = _make_dsn(self.db_user, self.db_password, port, self.dbname)
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
