import glob
import hashlib
import json
import os
import pickle
import subprocess
import time
import warnings
from datetime import date, datetime
from urllib.parse import quote_plus

import pandas as pd
import connectorx as cx
from sqlalchemy import create_engine, text

from .tunnel import find_cloudflared

DEFAULT_DBNAME = "quantdb"
DEFAULT_HOSTNAME = "shquantdb.alphawaves.vip"
DEFAULT_LOCAL_PORT = 15432
DEFAULT_POSTGRES_PORT = 5432  # local_host=True 일 때 직결할 로컬 Postgres 포트
# 인트라데이(ts) 패널의 표시 타임존. connectorx 가 timestamptz 를 naive-UTC 로 떨구므로
# SQL 에서 이 TZ 의 wall-clock 으로 투영한다 (모든 ts 패널이 KR 장중 데이터 → KST).
INTRADAY_TZ = "Asia/Seoul"


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


def _cache_sig(full, fields, tickers, start, end):
    """fetch_timeseries 결과를 유일하게 식별하는 짧은 해시.

    relation + fields + tickers + start/end 를 정규화(정렬·None 센티넬)해서 hash.
    fields/tickers 는 정렬 → 순서만 다른 호출이 같은 캐시를 공유한다.
    """
    norm = {
        "rel": full,
        "fields": "auto" if fields is None
                  else sorted([fields] if isinstance(fields, str) else fields),
        "tickers": "all" if tickers is None
                   else sorted([tickers] if isinstance(tickers, str) else tickers),
        "start": "" if start is None else str(start),
        "end": "" if end is None else str(end),
    }
    blob = json.dumps(norm, sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()[:12]


def _sql_lit(v):
    """connectorx 는 :name 바인드 미지원 → SQL 에 값을 인라인할 때 쓰는 Postgres 문자열 리터럴.

    작은따옴표를 2배로 escape (injection 방어). 식별자(테이블/컬럼명)가 아니라 값에만 쓴다.
    """
    return "'" + str(v).replace("'", "''") + "'"


def _is_date_only(v):
    """end 경계값이 '시각 없는 날짜' 인가.

    True 면 인트라데이(ts) 패널에서 그날 장 끝까지(end-of-day) 포함시킨다 — 'ts <= 날짜' 는
    자정(00:00) 경계라 그날 봉(09:00~)이 통째로 빠진다. datetime(시각 포함) 이나 'HH:MM' 가
    든 문자열은 명시 시각으로 보고 정확 경계(<=)를 유지한다(False).
    """
    if isinstance(v, datetime):       # datetime 은 date 의 하위클래스 → 먼저 검사
        return False
    if isinstance(v, date):
        return True
    return ":" not in str(v)          # 문자열: 시각 표기(:) 없으면 날짜-only


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
        self._dsn = None

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

    def _bulk_read(self, sql):
        """connectorx 로 SQL 한 방을 pandas DataFrame 으로 bulk read (대용량 fetch 용).

        connectorx(Rust 멀티코어)가 self._dsn 으로 자기 커넥션을 (이미 떠 있는) 터널에 직접 연다 —
        sqlalchemy verified engine/pool 우회. connectorx 는 :name 바인드 미지원 → 호출측이 값을
        _sql_lit 로 인라인해서 넘긴다.
        """
        if self._dsn is None:
            raise RuntimeError("QuantDB 는 `with QuantDB(...) as db:` 컨텍스트 안에서 사용하세요.")
        return cx.read_sql(self._dsn, sql, return_type="pandas")

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

    def fetch_timeseries(self, relation, fields=None, tickers=None, start=None, end=None,
                         save_and_reload_pickle_cache=False, verbose=True):
        """ai_ready timeseries 패널 객체(table/view/matview)를 wide pivot DataFrame 으로 가져온다.

        반환: index=시간(date/ts 자동), columns=MultiIndex(item=값필드, ticker, name, isin)
        (식별자는 객체에 존재하는 것만; fx_daily 는 iso_code/currency_name).

        - relation: ai_ready 객체명. relkind 무관 — table/view/matview/foreign 모두 가능 (pg_catalog 컬럼감지).
        - fields: 값 컬럼 리스트 (None 이면 numeric 측정값 자동 — *id/식별자 제외).
        - tickers: 엔티티(ticker, fx 는 iso_code) 필터 리스트. None 이면 전체.
        - start/end: 시간 범위 (시간 컬럼 기준).
        - save_and_reload_pickle_cache=True: pickle_cache/ 에 당일 캐시. 전체 파라미터(relation/fields/
          tickers/start/end) 해시를 키로 — 다른 호출끼리 충돌 없음. 0행 결과는 캐시하지 않음.
        - verbose=True: fetch 시작시각/완료(걸린시간) + 결과 요약 + 패널 print, 미필터 경고, 캐시 로드/저장. False=조용히 DataFrame 만.
        """
        full = relation if "." in relation else f"ai_ready.{relation}"
        schema, relname = full.split(".", 1)

        t_start = time.time()
        if verbose:
            print(f"fetch_timeseries({full}): fetch 시작 {time.strftime('%H:%M:%S')}")

        cache_file = None
        if save_and_reload_pickle_cache:
            cache_dir = "pickle_cache"
            os.makedirs(cache_dir, exist_ok=True)
            prefix = full.replace(".", "_")
            today_str = date.today().strftime("%Y%m%d")
            for f in glob.glob(os.path.join(cache_dir, f"{prefix}_*.pkl")):   # broad: 이 relation 의 과거 날짜 캐시 정리
                if today_str not in os.path.basename(f):
                    os.remove(f)
            cache_file = os.path.join(
                cache_dir, f"{prefix}_{_cache_sig(full, fields, tickers, start, end)}_{today_str}.pkl")
            if os.path.exists(cache_file):
                with open(cache_file, "rb") as fh:
                    cached = pickle.load(fh)
                if verbose:
                    print(f"pickle cache load: {cache_file}")
                    print(f"fetch_timeseries({full}): 완료 ({time.time() - t_start:.2f}s)")
                return cached

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

        conditions = []                                  # connectorx 는 :name 바인드 미지원 → 값 인라인(_sql_lit)
        if tickers is not None:
            tlist = [tickers] if isinstance(tickers, str) else list(tickers)
            conditions.append(f"{entity} IN ({', '.join(_sql_lit(t) for t in tlist)})")
        if start is not None:
            conditions.append(f"{time_col} >= {_sql_lit(start)}")
        if end is not None:
            if time_col == "ts" and _is_date_only(end):
                # 날짜-only end 를 인트라데이(ts) 패널에서 그날 끝까지 inclusive (다음날 자정 미만).
                # 'ts <= 날짜' 는 자정 경계라 그날 인트라데이 봉이 통째로 빠지므로 금지.
                conditions.append(f"{time_col} < ({_sql_lit(end)}::date + 1)")
            else:
                conditions.append(f"{time_col} <= {_sql_lit(end)}")
        if not conditions:
            warnings.warn(f"fetch_timeseries({full}): tickers/기간 미지정 — 전체 fetch (대용량 위험).")

        # connectorx 는 timestamptz 를 naive-UTC 로 반환 → ts 패널은 KST wall-clock 으로 투영해
        # 09:05~15:30 KST 로 나오게 한다 (일봉 date 패널은 tz 개념 없음 → 그대로).
        time_select = (f"({time_col} AT TIME ZONE '{INTRADAY_TZ}') AS {time_col}"
                       if time_col == "ts" else time_col)
        select = ", ".join([time_select] + identity + value_cols)
        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        long = self._bulk_read(f"SELECT {select} FROM {full}{where} ORDER BY {time_col}")   # connectorx → pandas
        if long.empty:
            if verbose:
                print(f"fetch_timeseries({full}): 0행 (조건에 맞는 데이터 없음).")
                print(f"fetch_timeseries({full}): 완료 ({time.time() - t_start:.2f}s)")
            return long

        long[time_col] = pd.to_datetime(long[time_col])
        # pandas pivot — wide(수만 열) 패널에서 polars melt-pivot 보다 압도적으로 빠름 (벤치: 6.8s vs 773s/13.5M행).
        wide = long.pivot(index=time_col, columns=identity, values=value_cols)
        wide.columns = wide.columns.set_names(["item"] + identity)
        if verbose:
            print(f"fetch_timeseries({full}): {wide.shape[0]:,}행 x {wide.shape[1]:,}열 "
                  f"[{wide.index.min()} ~ {wide.index.max()}], fields={value_cols}")
            print(wide)                                  # result 도 print (pandas 기본 잘림 — wide panel 폭주 방지)
        if cache_file is not None:                       # 0행은 위에서 early-return → 비결과 미캐시
            with open(cache_file, "wb") as fh:
                pickle.dump(wide, fh)
            if verbose:
                print(f"pickle cache save: {cache_file}")
        if verbose:
            print(f"fetch_timeseries({full}): 완료 ({time.time() - t_start:.2f}s)")
        return wide

    def __enter__(self):
        if not self.local_host:                       # local_host 면 터널 없이 로컬 Postgres 직결
            self._tunnel = self._start_tunnel()
        try:
            port = DEFAULT_POSTGRES_PORT if self.local_host else self.local_port
            dsn = _make_dsn(self.db_user, self.db_password, port, self.dbname)
            self._dsn = dsn                           # connectorx bulk read 가 재사용 (sqlalchemy engine 우회)
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
