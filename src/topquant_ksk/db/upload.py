import io
import os
import time
import xlwings as xw
import pandas as pd
import polars as pl
from datetime import datetime as _dt
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from .tunnel import manage_db_tunnel, kill_tunnel


def _pandas_dtype_to_pg(dtype):
    """pandas dtype → PostgreSQL 타입 매핑"""
    s = str(dtype)
    if 'float' in s:
        return 'DOUBLE PRECISION'
    if 'int' in s:
        return 'BIGINT'
    if 'datetime' in s:
        return 'TIMESTAMPTZ'
    if 'bool' in s:
        return 'BOOLEAN'
    if s == 'object':
        return 'TEXT'
    return 'DOUBLE PRECISION'


def _resolve_table(cur, table_name):
    """테이블이 이미 존재하면 schema.table_name 반환, 없으면 None. 스키마 명시(schema.table) 시 해당 스키마에서만 확인. 미명시 시 자동 탐색하며, 여러 스키마에 존재하면 오류."""
    if '.' in table_name:
        schema, name = table_name.split('.', 1)
        cur.execute(
            "SELECT 1 FROM pg_tables WHERE schemaname = %s AND tablename = %s",
            (schema, name)
        )
        return table_name if cur.fetchone() else None
    cur.execute(
        "SELECT schemaname FROM pg_tables WHERE tablename = %s",
        (table_name,)
    )
    rows = cur.fetchall()
    if len(rows) > 1:
        schemas = [r[0] for r in rows]
        raise ValueError(f"테이블 '{table_name}'이 여러 스키마에 존재합니다: {schemas}. schema.table_name 형식으로 지정해주세요.")
    if rows:
        return f"{rows[0][0]}.{table_name}"
    return None


def refresh_materialized_view_concurrently(
    table_name: str,
    source_tables: list[str] = None,
    join_keys: list[str] = None,
    unique_index_cols: list[str] = None,
    db_user: str = None,
    db_password: str = None,
    local_host: bool = False,
):
    """
    Materialized View를 갱신. source_tables의 컬럼이 변경되면 자동으로 DROP+CREATE.

    Parameters:
    - table_name: MV 이름 (e.g. 'daily_adjusted_time_series_data_stock')
    - source_tables: JOIN할 source 테이블 리스트 (e.g. ['private.private_daily_adjusted_...', 'private.private_daily_fixed_...'])
    - join_keys: JOIN 조건 컬럼 (e.g. ['time', 'sedol']). COALESCE 대상이 됨.
    - unique_index_cols: CONCURRENTLY refresh용 unique index 컬럼 (e.g. ['time', 'sedol'])
    - db_user, db_password, local_host: DB 연결 정보
    """
    tunnel_proc = None
    try:
        if not local_host:
            tunnel_proc = manage_db_tunnel()
            if tunnel_proc is None:
                print("🚨 터널 연결 실패.")
                return

        pw_encoded = quote_plus(db_password)
        port = 5432 if local_host else 15432
        engine = create_engine(f"postgresql://{db_user}:{pw_encoded}@127.0.0.1:{port}/quant_data")

        full_name = table_name if '.' in table_name else f"public.{table_name}"

        with engine.connect() as conn:
            need_recreate = False

            if source_tables and join_keys:
                # MV 현재 컬럼 조회
                mv_schema, mv_name = full_name.split('.', 1)
                mv_cols_result = conn.execute(text("""
                    SELECT a.attname FROM pg_attribute a
                    JOIN pg_class c ON a.attrelid = c.oid
                    JOIN pg_namespace n ON c.relnamespace = n.oid
                    WHERE n.nspname = :schema AND c.relname = :table
                      AND a.attnum > 0 AND NOT a.attisdropped
                    ORDER BY a.attnum
                """), {"schema": mv_schema, "table": mv_name})
                mv_cols = {row[0] for row in mv_cols_result}

                # source 테이블들의 전체 컬럼 조회
                all_source_cols = set()
                for src in source_tables:
                    src_schema, src_name = src.split('.', 1)
                    src_result = conn.execute(text("""
                        SELECT a.attname FROM pg_attribute a
                        JOIN pg_class c ON a.attrelid = c.oid
                        JOIN pg_namespace n ON c.relnamespace = n.oid
                        WHERE n.nspname = :schema AND c.relname = :table
                          AND a.attnum > 0 AND NOT a.attisdropped
                    """), {"schema": src_schema, "table": src_name})
                    all_source_cols.update(row[0] for row in src_result)

                if all_source_cols != mv_cols:
                    new_cols = all_source_cols - mv_cols
                    print(f"  MV 컬럼 변경 감지 (신규: {new_cols}). DROP + CREATE 수행...")
                    need_recreate = True

            if need_recreate:
                # 각 source 테이블의 고유 컬럼 조회
                aliases = [chr(ord('a') + i) for i in range(len(source_tables))]
                table_cols = {}
                for src, alias in zip(source_tables, aliases):
                    src_schema, src_name = src.split('.', 1)
                    src_result = conn.execute(text("""
                        SELECT a.attname FROM pg_attribute a
                        JOIN pg_class c ON a.attrelid = c.oid
                        JOIN pg_namespace n ON c.relnamespace = n.oid
                        WHERE n.nspname = :schema AND c.relname = :table
                          AND a.attnum > 0 AND NOT a.attisdropped
                        ORDER BY a.attnum
                    """), {"schema": src_schema, "table": src_name})
                    table_cols[alias] = [row[0] for row in src_result]

                # SELECT 절 생성: 공통 키는 COALESCE, 나머지는 각 테이블에서
                # COALESCE 대상: 모든 source에 공통으로 존재하는 컬럼
                common_cols = set(table_cols[aliases[0]])
                for alias in aliases[1:]:
                    common_cols &= set(table_cols[alias])

                select_parts = []
                for col in common_cols:
                    coalesce_args = ", ".join(f"{a}.{col}" for a in aliases)
                    select_parts.append(f"COALESCE({coalesce_args}) AS {col}")

                added = set()
                for alias in aliases:
                    for col in table_cols[alias]:
                        if col not in common_cols and col not in added:
                            select_parts.append(f"{alias}.{col}")
                            added.add(col)

                select_sql = ",\n            ".join(select_parts)

                # FROM + JOIN 절 생성
                from_sql = f"{source_tables[0]} {aliases[0]}"
                for src, alias in zip(source_tables[1:], aliases[1:]):
                    join_cond = " AND ".join(f"{aliases[0]}.{k} = {alias}.{k}" for k in join_keys)
                    from_sql += f"\n        FULL OUTER JOIN {src} {alias} ON {join_cond}"

                # DROP + CREATE
                conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {full_name}"))
                create_sql = f"""
                    CREATE MATERIALIZED VIEW {full_name} AS
                    SELECT {select_sql}
                    FROM {from_sql}
                """
                conn.execute(text(create_sql))

                # unique index 재생성
                if unique_index_cols:
                    idx_name = f"idx_mv_{'_'.join(mv_name.split('_')[:3])}_{('_'.join(unique_index_cols))}"
                    idx_cols = ", ".join(unique_index_cols)
                    conn.execute(text(f"CREATE UNIQUE INDEX {idx_name} ON {full_name} ({idx_cols})"))

                conn.commit()
                print(f"Materialized View 재생성 완료: {full_name}")
            else:
                conn.execute(text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {full_name}"))
                conn.commit()
                print(f"Materialized View 갱신 완료: {full_name}")
    finally:
        if tunnel_proc is not None:
            kill_tunnel(tunnel_proc)


def run_factset_refresh_N_save_to_csv(file_path, refresh_master_table=False, only_listed=False):
    _ts = lambda: _dt.now().strftime('%H:%M:%S')
    monitor_row = "A8:CZ8" if refresh_master_table else "A7:CZ7"
    step = 0

    def next_step():
        nonlocal step
        step += 1
        return step

    def _wait_for_calc(sheet, row_range, step_num):
        print(f"[{_ts()}] ⏳ {step_num}. 데이터 계산 완료 확인 중 (#Calc 탈출 감시, {row_range})...")
        for i in range(180):
            row_vals = sheet.range(row_range).value
            str_vals = [str(v).strip() for v in row_vals if v is not None]
            if all("#Calc" not in v for v in str_vals) and len(str_vals) > 0:
                print(f"[{_ts()}] 🎉 데이터 갱신 확인 완료! (소요 시간: {i}초)")
                return
            time.sleep(1)
        print(f"[{_ts()}] ⏰ 경고: 계산 대기 시간이 초과되었습니다.")

    # 1. FactSet Fix Excel 실행 (Add-in 안정화)
    FIXEXCEL_PATH = r"C:\Program Files (x86)\FactSet\fdswFixExcel.exe"
    if os.path.exists(FIXEXCEL_PATH):
        print(f"[{_ts()}] 🔧 {next_step()}. FactSet Fix Excel 실행 중...")
        os.startfile(FIXEXCEL_PATH)
        time.sleep(3)

    file_name = os.path.basename(file_path)
    print(f"[{_ts()}] 🚀 {next_step()}. {file_name} 실행 및 로드 대기...")
    os.startfile(file_path)

    # 2. 파일 연결 확인 (Logical Blocking)
    start_time = time.time()
    app, wb = None, None
    while time.time() - start_time < 300:
        try:
            for active_app in xw.apps:
                if file_name in [b.name for b in active_app.books]:
                    app = active_app
                    wb = app.books[file_name]
                    for b in active_app.books:
                        if b.name.startswith("Book") and b.name != file_name:
                            b.close()
                    break
            if wb: break
        except: pass
        time.sleep(1)

    if not wb: raise TimeoutError(f"[{_ts()}] 🔥 엑셀 파일 연결에 실패했습니다.")

    try:
        # 3. only_listed: DB에서 상장 종목만 ticker 시트에 반영
        if only_listed:
            print(f"[{_ts()}] 📋 {next_step()}. DB 상장 종목 필터 적용 (FetchMasterTable_listed_from_StartDate)...")
            app.activate(steal_focus=True)
            app.api.Run("PERSONAL.XLSB!FetchMasterTable_listed_from_StartDate")

        # FactSet 공식 매크로 호출 (전체 재계산)
        print(f"[{_ts()}] ⚡ {next_step()}. FactSet 전체 재계산 실행 (FDS_RECALC_NOW)...")
        time.sleep(1)
        app.activate(steal_focus=True)
        app.api.Run("FDS_RECALC_NOW")

        # 4. 데이터 확정 모니터링
        sheet = wb.sheets[-1]
        _wait_for_calc(sheet, monitor_row, next_step())

        # 5~7. refresh_master_table 전용 단계
        if refresh_master_table:
            print(f"[{_ts()}] 🔄 {next_step()}. Fill_Rows_Based_On_Row8 매크로 실행...")
            app.api.Run("PERSONAL.XLSB!Fill_Rows_Based_On_Row8")

            print(f"[{_ts()}] ⚡ {next_step()}. FactSet 전체 재계산 재실행 (FDS_RECALC_NOW)...")
            app.api.Run("FDS_RECALC_NOW")

            _wait_for_calc(sheet, monitor_row, next_step())

        # 스마트 카피 매크로 실행
        print(f"[{_ts()}] ⏳ 데이터 안정화 대기 (5초)...")
        time.sleep(5)
        print(f"[{_ts()}] 🚀 {next_step()}. 스마트 카피 매크로 실행 (CSV 내보내기)...")
        app.api.Run("PERSONAL.XLSB!Export_FactSet_Smart_Copy")

        # 프로세스 종료
        wb.close()
        print(f"[{_ts()}] 💾 {next_step()}. 작업 파일 닫기 및 모든 자동화 프로세스 종료.")

    except Exception as e:
        print(f"[{_ts()}] 🔥 에러 발생: {e}")

def upload_index_DataFrame_with_polars(df: pd.DataFrame,  db_user: str, db_password: str, local_host=False, table_name: str = "adjusted_time_series_data_index", truncate=False):
    """
    Index DataFrame (MultiIndex columns)을 DB에 Upsert.
    테이블이 없으면 자동 생성.

    Parameters:
    - df: pandas DataFrame with MultiIndex columns, index=time
          지원 형식:
          - 3-level: (ticker, index_name, item_name)
          - 2-level: (ticker, item_name) → index_name은 NULL로 저장
    - table_name: Target table name
    - truncate: True이면 insert 전에 테이블을 TRUNCATE (기존 데이터 전체 삭제)
    """
    tunnel_proc = None
    try:
        if not local_host:
            tunnel_proc = manage_db_tunnel()
            if tunnel_proc is None:
                print("🚨 터널 연결 실패.")
                return None

        port = 5432 if local_host else 15432
        uri = f"postgresql://{db_user}:{db_password}@127.0.0.1:{port}/quant_data"
        engine = create_engine(uri)

        INDEX_COL_MAP = {
            "FG_PRICE_OPEN": "open",
            "FG_PRICE_LOW": "low",
            "FG_PRICE_HIGH": "high",
            "FG_PRICE": "close_pr",
            "FG_TOTAL_RET_IDX": "close_tr",
        }

        value_names = list(INDEX_COL_MAP.values())

        print(f"[{_dt.now().strftime('%H:%M:%S')}] 🚀 Upload 시작: {table_name}")

        # 1. 기존 테이블 확인 / 없으면 생성
        print(f"  [{_dt.now().strftime('%H:%M:%S')}] [1/4] 테이블 확인/생성 중...")
        raw_name = table_name.split(".")[-1]
        conn = engine.raw_connection()
        try:
            with conn.cursor() as cur:
                resolved = _resolve_table(cur, table_name)
                if resolved:
                    table_name = resolved
                    print(f"        - 기존 테이블 발견: {table_name}")
                else:
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            time       TIMESTAMPTZ      NOT NULL,
                            ticker     TEXT             NOT NULL,
                            index_name TEXT,
                            open       DOUBLE PRECISION,
                            low        DOUBLE PRECISION,
                            high       DOUBLE PRECISION,
                            close_pr   DOUBLE PRECISION,
                            close_tr   DOUBLE PRECISION,
                            PRIMARY KEY (time, ticker)
                        );
                    """)
                    cur.execute(f"""
                        SELECT EXISTS (
                            SELECT 1 FROM timescaledb_information.hypertables
                            WHERE hypertable_name = '{raw_name}'
                        );
                    """)
                    is_hypertable = cur.fetchone()[0]
                    if not is_hypertable:
                        cur.execute(f"SELECT create_hypertable('{table_name}', 'time', if_not_exists => TRUE);")
                        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{raw_name}_ticker ON {table_name} (ticker);")
                    print(f"        - 새 테이블 생성: {table_name}")
                conn.commit()
        finally:
            conn.close()

        # 2. DataFrame → Polars 변환
        print(f"  [{_dt.now().strftime('%H:%M:%S')}] [2/4] DataFrame 변환 중...")
        print(f"        - 기간: {df.index.min()} ~ {df.index.max()}")
        df_copy = df.copy()

        # ticker → index_name 매핑 추출 & columns flatten
        if df_copy.columns.nlevels == 3:
            # (ticker, index_name, item_name)
            ticker_index_name_map = {ticker: idx_name for ticker, idx_name, _ in df_copy.columns}
            df_copy.columns = [f"{ticker}|{INDEX_COL_MAP[item_name]}" for ticker, _, item_name in df_copy.columns]
        else:
            # (ticker, item_name)
            ticker_index_name_map = {}
            df_copy.columns = [f"{ticker}|{INDEX_COL_MAP[item_name]}" for ticker, item_name in df_copy.columns]

        p_df = pl.from_pandas(df_copy.reset_index())
        p_long = p_df.unpivot(index="index", variable_name="info", value_name="value")
        p_long = p_long.filter(pl.col("value").is_not_null())
        p_long = p_long.with_columns(
            pl.col("info").str.split("|").alias("_split")
        ).with_columns([
            pl.col("_split").list.get(0).alias("ticker"),
            pl.col("_split").list.get(1).alias("item_name"),
        ]).drop("_split", "info")

        # 3. Pivot → wide format + index_name join
        print(f"  [{_dt.now().strftime('%H:%M:%S')}] [3/4] Pivot 변환 중...")
        wide_df = p_long.pivot(values="value", index=["index", "ticker"], on="item_name")

        # ticker별 항목 수가 다를 수 있으므로 없는 컬럼은 null로 추가
        for col in value_names:
            if col not in wide_df.columns:
                wide_df = wide_df.with_columns(pl.lit(None).cast(pl.Float64).alias(col))

        # index_name 매핑 join
        if ticker_index_name_map:
            map_df = pl.DataFrame({"ticker": list(ticker_index_name_map.keys()), "index_name": list(ticker_index_name_map.values())})
            wide_df = wide_df.join(map_df, on="ticker", how="left")
        else:
            wide_df = wide_df.with_columns(pl.lit(None).cast(pl.Utf8).alias("index_name"))

        final_df = wide_df.rename({"index": "time"}).select(["time", "ticker", "index_name"] + value_names)
        print(f"        - 최종 데이터: {len(final_df):,}건")

        # 4. CSV 버퍼 → COPY → UPSERT
        print(f"  [4/4] DB {'Truncate + Insert' if truncate else 'Upsert'} 실행 중... ({_dt.now().strftime('%H:%M:%S')})")
        buffer = io.BytesIO()
        final_df.write_csv(buffer, include_header=False, separator='\t')
        buffer.seek(0)

        all_cols = ["time", "ticker", "index_name"] + value_names
        update_cols_list = ["index_name"] + value_names
        conn = engine.raw_connection()
        try:
            with conn.cursor() as cur:
                if truncate:
                    df_min_time = final_df["time"].min()
                    if hasattr(df_min_time, 'replace'):
                        df_min_time = df_min_time.replace(tzinfo=None)
                    cur.execute(f"SELECT MIN(time) FROM {table_name}")
                    db_min_time = cur.fetchone()[0]
                    if db_min_time is not None and df_min_time > db_min_time.replace(tzinfo=None):
                        print(f"⚠️ Truncate 거부: 새 데이터 시작({df_min_time}) > DB 시작({db_min_time}). Upsert로 전환.")
                        truncate = False

                if truncate:
                    cur.execute(f"TRUNCATE {table_name}")
                    cols_str = ", ".join(all_cols)
                    cur.copy_expert(f"COPY {table_name} ({cols_str}) FROM STDIN WITH (DELIMITER E'\\t', NULL '')", buffer)
                else:
                    temp_name = f"temp_{raw_name}"
                    cur.execute(f"CREATE TEMP TABLE {temp_name} (LIKE {table_name} INCLUDING DEFAULTS) ON COMMIT DROP")
                    cur.copy_from(buffer, temp_name, sep="\t", null="", columns=all_cols)

                    update_cols = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols_list])
                    cols_str = ", ".join(all_cols)
                    upsert_query = f"""
                    INSERT INTO {table_name} ({cols_str})
                    SELECT {cols_str} FROM {temp_name}
                    ON CONFLICT (time, ticker) DO UPDATE SET
                        {update_cols};
                    """
                    cur.execute(upsert_query)
                conn.commit()
                print(f"✅ 완료! {len(final_df):,}건 {'Insert' if truncate else 'Upsert'} 성공 ({_dt.now().strftime('%H:%M:%S')})")
        finally:
            conn.close()

        return final_df

    finally:
        if tunnel_proc is not None:
            kill_tunnel(tunnel_proc)


def upload_index_macro_DataFrame_with_polars(
    df: pd.DataFrame,
    col_map: dict,
    table_name: str,
    db_user: str,
    db_password: str,
    local_host=False,
    truncate=False,
):
    """
    Index/Macro DataFrame (MultiIndex columns)을 DB에 Upsert.
    테이블이 없으면 자동 생성, 누락 컬럼은 자동 추가.

    Parameters:
    - df: pandas DataFrame with MultiIndex columns, index=time
          지원 형식:
          - 3-level: (ticker, index_name, item_name)
          - 2-level: (ticker, item_name) → index_name은 NULL로 저장
    - col_map: item_name → DB 컬럼명 매핑 (e.g. {"FG_YIELD": "ytm"})
    - table_name: Target table name (e.g. "public.macro_time_series")
    - truncate: True이면 insert 전에 테이블을 TRUNCATE (기존 데이터 전체 삭제)
    """
    tunnel_proc = None
    try:
        if not local_host:
            tunnel_proc = manage_db_tunnel()
            if tunnel_proc is None:
                print("🚨 터널 연결 실패.")
                return None

        port = 5432 if local_host else 15432
        uri = f"postgresql://{db_user}:{db_password}@127.0.0.1:{port}/quant_data"
        engine = create_engine(uri)

        value_names = list(col_map.values())

        print(f"[{_dt.now().strftime('%H:%M:%S')}] 🚀 Upload 시작: {table_name}")

        # 1. 기존 테이블 확인 / 없으면 생성
        print(f"  [{_dt.now().strftime('%H:%M:%S')}] [1/5] 테이블 확인/생성 중...")
        raw_name = table_name.split(".")[-1]
        conn = engine.raw_connection()
        try:
            with conn.cursor() as cur:
                resolved = _resolve_table(cur, table_name)
                if resolved:
                    table_name = resolved
                    print(f"        - 기존 테이블 발견: {table_name}")
                    # 누락 컬럼 자동 추가
                    cur.execute(
                        "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
                        (table_name.split('.')[0], raw_name)
                    )
                    existing_cols = {row[0] for row in cur.fetchall()}
                    for col in value_names:
                        if col not in existing_cols:
                            pg_type = _pandas_dtype_to_pg(df.dtypes.iloc[0])
                            cur.execute(f'ALTER TABLE {table_name} ADD COLUMN {col} {pg_type}')
                            print(f"        - 컬럼 추가: {col} ({pg_type})")
                else:
                    value_cols_sql = ",\n                            ".join([f"{col} DOUBLE PRECISION" for col in value_names])
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            time       TIMESTAMPTZ      NOT NULL,
                            ticker     TEXT             NOT NULL,
                            index_name TEXT,
                            {value_cols_sql},
                            PRIMARY KEY (time, ticker)
                        );
                    """)
                    cur.execute(f"""
                        SELECT EXISTS (
                            SELECT 1 FROM timescaledb_information.hypertables
                            WHERE hypertable_name = '{raw_name}'
                        );
                    """)
                    is_hypertable = cur.fetchone()[0]
                    if not is_hypertable:
                        cur.execute(f"SELECT create_hypertable('{table_name}', 'time', if_not_exists => TRUE);")
                        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{raw_name}_ticker ON {table_name} (ticker);")
                    print(f"        - 새 테이블 생성: {table_name}")
                conn.commit()
        finally:
            conn.close()

        # 2. DataFrame → Polars 변환
        print(f"  [{_dt.now().strftime('%H:%M:%S')}] [2/5] DataFrame 변환 중...")
        print(f"        - 기간: {df.index.min()} ~ {df.index.max()}")
        df_copy = df.copy()

        # ticker → index_name 매핑 추출 & columns flatten
        if df_copy.columns.nlevels == 3:
            ticker_index_name_map = {ticker: idx_name for ticker, idx_name, _ in df_copy.columns}
            df_copy.columns = [f"{ticker}|{col_map[item_name]}" for ticker, _, item_name in df_copy.columns]
        else:
            ticker_index_name_map = {}
            df_copy.columns = [f"{ticker}|{col_map[item_name]}" for ticker, item_name in df_copy.columns]

        p_df = pl.from_pandas(df_copy.reset_index())
        p_long = p_df.unpivot(index="index", variable_name="info", value_name="value")
        p_long = p_long.filter(pl.col("value").is_not_null())
        p_long = p_long.with_columns(
            pl.col("info").str.split("|").alias("_split")
        ).with_columns([
            pl.col("_split").list.get(0).alias("ticker"),
            pl.col("_split").list.get(1).alias("item_name"),
        ]).drop("_split", "info")

        # 3. Pivot → wide format + index_name join
        print(f"  [{_dt.now().strftime('%H:%M:%S')}] [3/5] Pivot 변환 중...")
        wide_df = p_long.pivot(values="value", index=["index", "ticker"], on="item_name")

        for col in value_names:
            if col not in wide_df.columns:
                wide_df = wide_df.with_columns(pl.lit(None).cast(pl.Float64).alias(col))

        if ticker_index_name_map:
            map_df = pl.DataFrame({"ticker": list(ticker_index_name_map.keys()), "index_name": list(ticker_index_name_map.values())})
            wide_df = wide_df.join(map_df, on="ticker", how="left")
        else:
            wide_df = wide_df.with_columns(pl.lit(None).cast(pl.Utf8).alias("index_name"))

        final_df = wide_df.rename({"index": "time"}).select(["time", "ticker", "index_name"] + value_names)
        print(f"        - 최종 데이터: {len(final_df):,}건")

        # 4. DB integer/boolean 컬럼 자동 캐스팅
        print(f"  [{_dt.now().strftime('%H:%M:%S')}] [4/5] 타입 캐스팅 확인 중...")
        with engine.connect() as type_conn:
            for col in value_names:
                result = type_conn.execute(text(
                    "SELECT data_type FROM information_schema.columns "
                    "WHERE table_schema = :schema AND table_name = :table AND column_name = :col"
                ), {"schema": table_name.split('.')[0], "table": raw_name, "col": col})
                row = result.fetchone()
                if row and row[0] in ('bigint', 'integer', 'smallint'):
                    final_df = final_df.with_columns(pl.col(col).cast(pl.Int64))
                elif row and row[0] == 'boolean':
                    final_df = final_df.with_columns(pl.col(col).cast(pl.Boolean))

        # 5. CSV 버퍼 → COPY → UPSERT
        print(f"  [5/5] DB {'Truncate + Insert' if truncate else 'Upsert'} 실행 중... ({_dt.now().strftime('%H:%M:%S')})")
        buffer = io.BytesIO()
        final_df.write_csv(buffer, include_header=False, separator='\t')
        buffer.seek(0)

        all_cols = ["time", "ticker", "index_name"] + value_names
        update_cols_list = ["index_name"] + value_names
        conn = engine.raw_connection()
        try:
            with conn.cursor() as cur:
                if truncate:
                    df_min_time = final_df["time"].min()
                    if hasattr(df_min_time, 'replace'):
                        df_min_time = df_min_time.replace(tzinfo=None)
                    cur.execute(f"SELECT MIN(time) FROM {table_name}")
                    db_min_time = cur.fetchone()[0]
                    if db_min_time is not None and df_min_time > db_min_time.replace(tzinfo=None):
                        print(f"⚠️ Truncate 거부: 새 데이터 시작({df_min_time}) > DB 시작({db_min_time}). Upsert로 전환.")
                        truncate = False

                if truncate:
                    cur.execute(f"TRUNCATE {table_name}")
                    cols_str = ", ".join(all_cols)
                    cur.copy_expert(f"COPY {table_name} ({cols_str}) FROM STDIN WITH (DELIMITER E'\\t', NULL '')", buffer)
                else:
                    temp_name = f"temp_{raw_name}"
                    cur.execute(f"CREATE TEMP TABLE {temp_name} (LIKE {table_name} INCLUDING DEFAULTS) ON COMMIT DROP")
                    cur.copy_from(buffer, temp_name, sep="\t", null="", columns=all_cols)

                    update_cols = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols_list])
                    cols_str = ", ".join(all_cols)
                    upsert_query = f"""
                    INSERT INTO {table_name} ({cols_str})
                    SELECT {cols_str} FROM {temp_name}
                    ON CONFLICT (time, ticker) DO UPDATE SET
                        {update_cols};
                    """
                    cur.execute(upsert_query)
                conn.commit()
                print(f"✅ 완료! {len(final_df):,}건 {'Insert' if truncate else 'Upsert'} 성공 ({_dt.now().strftime('%H:%M:%S')})")
        finally:
            conn.close()

        return final_df

    finally:
        if tunnel_proc is not None:
            kill_tunnel(tunnel_proc)


def upload_stock_timeseries_DataFrame_with_polars(dfs: list,
                                                  value_names: list,
                                                  table_name: str,                                                    
                                                  db_user: str, 
                                                  db_password: str, 
                                                  local_host=False, 
                                                  truncate=False):
    """
    Stock DataFrame 리스트를 DB에 Upsert.
    테이블이 없으면 자동 생성, 누락 컬럼은 자동 추가.

    Parameters:
    - dfs: List of pandas DataFrames (MultiIndex columns: ticker, company_name, sedol)
    - value_names: List of column names for values (e.g. ["close_pr", "close_tr"])
    - db_user: PostgreSQL 사용자명
    - db_password: PostgreSQL 비밀번호
    - local_host: True이면 터널 없이 localhost 직접 연결
    - table_name: 테이블명. schema.table 형식으로 스키마 명시 가능 (e.g. "private.private_daily_fixed_time_series_data_stock")
    - truncate: True이면 insert 전에 테이블을 TRUNCATE
    """
    if len(dfs) != len(value_names):
        raise ValueError("dfs와 value_names의 길이가 같아야 합니다.")

    value_names = [v.lower() for v in value_names]

    tunnel_proc = None
    try:
        if not local_host:
            tunnel_proc = manage_db_tunnel()
            if tunnel_proc is None:
                print("🚨 터널 연결 실패.")
                return None

        port = 5432 if local_host else 15432
        uri = f"postgresql://{db_user}:{db_password}@127.0.0.1:{port}/quant_data"
        engine = create_engine(uri)

        print(f"[{_dt.now().strftime('%H:%M:%S')}] 🚀 Upload 시작: {table_name} 테이블에 {value_names} 컬럼 업로드")

        # 1. 기존 테이블 확인 / 없으면 생성
        print(f"  [{_dt.now().strftime('%H:%M:%S')}] [1/5] 테이블 확인/생성 중...")
        raw_name = table_name.split(".")[-1]
        value_cols_sql = ",\n                        ".join([f"{col} DOUBLE PRECISION" for col in value_names])
        conn = engine.raw_connection()
        try:
            with conn.cursor() as cur:
                resolved = _resolve_table(cur, table_name)
                if resolved:
                    table_name = resolved
                    print(f"        - 기존 테이블 발견: {table_name}")
                    # 누락 컬럼 자동 추가 (dtype 자동 추론)
                    cur.execute(
                        "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
                        (table_name.split('.')[0], raw_name)
                    )
                    existing_cols = {row[0] for row in cur.fetchall()}
                    for df_input, col in zip(dfs, value_names):
                        if col not in existing_cols:
                            pg_type = _pandas_dtype_to_pg(df_input.dtypes.iloc[0])
                            cur.execute(f'ALTER TABLE {table_name} ADD COLUMN {col} {pg_type}')
                            print(f"        - 컬럼 추가: {col} ({pg_type})")
                else:
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            time         TIMESTAMPTZ NOT NULL,
                            ticker       TEXT        NOT NULL,
                            company_name TEXT,
                            sedol        TEXT        NOT NULL,
                            {value_cols_sql},
                            PRIMARY KEY (time, sedol)
                        );
                    """)
                    cur.execute(f"""
                        SELECT EXISTS (
                            SELECT 1 FROM timescaledb_information.hypertables
                            WHERE hypertable_name = '{raw_name}'
                        );
                    """)
                    is_hypertable = cur.fetchone()[0]
                    if not is_hypertable:
                        cur.execute(f"SELECT create_hypertable('{table_name}', 'time', if_not_exists => TRUE);")
                        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{raw_name}_sedol ON {table_name} (sedol);")
                    print(f"        - 새 테이블 생성: {table_name}")
                conn.commit()
        finally:
            conn.close()

        # 2. MultiIndex를 | 구분자 문자열로 변환
        print(f"  [{_dt.now().strftime('%H:%M:%S')}] [2/5] DataFrame을 Polars Long 형식으로 변환 중...")
        polars_longs = []
        active_value_names = []
        for df, value_name in zip(dfs, value_names):
            df_copy = df.copy()
            if df_copy.empty:
                print(f"        - {value_name}: 0건 (empty, 건너뜀)")
                continue
            df_copy.columns = [f"{ticker}|{company_name}|{sedol}" for ticker, company_name, sedol in df_copy.columns]
            p_df = pl.from_pandas(df_copy.reset_index().rename(columns={df_copy.index.name or "index": "time"}))
            p_df = p_df.with_columns(pl.col("time").cast(pl.Datetime("us", "UTC")))
            p_long = p_df.unpivot(index="time", variable_name="info", value_name=value_name)
            p_long = p_long.filter(pl.col(value_name).is_not_null())
            if len(p_long) == 0:
                print(f"        - {value_name}: 0건 (건너뜀)")
                continue
            polars_longs.append(p_long)
            active_value_names.append(value_name)
            print(f"        - {value_name}: {len(p_long):,}건")

        if not polars_longs:
            print(f"⚠️ 모든 DataFrame이 비어있습니다. 업로드 건너뜁니다.")
            return None

        # 3. Concat + Pivot 방식으로 병합
        print(f"  [{_dt.now().strftime('%H:%M:%S')}] [3/5] Concat + Pivot 병합 중...")
        dfs_with_type = []
        for p_long, value_name in zip(polars_longs, active_value_names):
            df_renamed = p_long.rename({value_name: "value"})
            df_with_type = df_renamed.with_columns(pl.lit(value_name).alias("value_type"))
            dfs_with_type.append(df_with_type)

        stacked = pl.concat(dfs_with_type)
        combined = stacked.pivot(values="value", index=["time", "info"], on="value_type")
        print(f"        - 병합 결과: {len(combined):,}건")

        # 4. Info 컬럼 분리
        print(f"  [{_dt.now().strftime('%H:%M:%S')}] [4/5] 컬럼 분리 및 정리 중...")
        final_df = combined.with_columns(
            pl.col("info").str.split("|").alias("_split")
        ).with_columns([
            pl.col("_split").list.get(0).alias("ticker"),
            pl.col("_split").list.get(1).alias("company_name"),
            pl.col("_split").list.get(2).alias("sedol"),
        ]).drop("_split", "info").select(
            ["time", "ticker", "company_name", "sedol"] + active_value_names
        )
        print(f"        - 최종 데이터: {len(final_df):,}건")

        # 4.5. DB integer 컬럼에 float 데이터가 들어오면 자동 캐스팅
        with engine.connect() as type_conn:
            for col in active_value_names:
                result = type_conn.execute(text(
                    "SELECT data_type FROM information_schema.columns "
                    "WHERE table_schema = :schema AND table_name = :table AND column_name = :col"
                ), {"schema": table_name.split('.')[0], "table": raw_name, "col": col})
                row = result.fetchone()
                if row and row[0] in ('bigint', 'integer', 'smallint'):
                    final_df = final_df.with_columns(pl.col(col).cast(pl.Int64))
                elif row and row[0] == 'boolean':
                    final_df = final_df.with_columns(pl.col(col).cast(pl.Boolean))

        # 5. CSV 버퍼 → COPY → UPSERT
        print(f"  [5/5] DB {'Truncate + Insert' if truncate else 'Upsert'} 실행 중... ({_dt.now().strftime('%H:%M:%S')})")
        buffer = io.BytesIO()
        final_df.write_csv(buffer, include_header=False, separator='\t')
        buffer.seek(0)

        all_cols = ["time", "ticker", "company_name", "sedol"] + active_value_names
        conn = engine.raw_connection()
        try:
            with conn.cursor() as cur:
                if truncate:
                    df_min_time = final_df["time"].min()
                    if hasattr(df_min_time, 'replace'):
                        df_min_time = df_min_time.replace(tzinfo=None)
                    cur.execute(f"SELECT MIN(time) FROM {table_name}")
                    db_min_time = cur.fetchone()[0]
                    if db_min_time is not None and df_min_time > db_min_time.replace(tzinfo=None):
                        print(f"⚠️ Truncate 거부: 새 데이터 시작({df_min_time}) > DB 시작({db_min_time}). Upsert로 전환.")
                        truncate = False

                if truncate:
                    cur.execute(f"TRUNCATE {table_name}")
                    cols_str = ", ".join(all_cols)
                    cur.copy_expert(f"COPY {table_name} ({cols_str}) FROM STDIN WITH (DELIMITER E'\\t', NULL '')", buffer)
                else:
                    temp_name = f"temp_{raw_name}"
                    cur.execute(f"CREATE TEMP TABLE {temp_name} (LIKE {table_name} INCLUDING DEFAULTS) ON COMMIT DROP")
                    cur.copy_from(buffer, temp_name, sep="\t", null="", columns=all_cols)

                    update_cols = ", ".join([f"{col} = EXCLUDED.{col}" for col in active_value_names + ["ticker", "company_name"]])
                    cols_str = ", ".join(all_cols)
                    upsert_query = f"""
                    INSERT INTO {table_name} ({cols_str})
                    SELECT {cols_str} FROM {temp_name}
                    ON CONFLICT (time, sedol) DO UPDATE SET
                        {update_cols};
                    """
                    cur.execute(upsert_query)
                conn.commit()
                print(f"✅ 완료! {len(final_df):,}건 {'Insert' if truncate else 'Upsert'} 성공 ({_dt.now().strftime('%H:%M:%S')})")
        finally:
            conn.close()

        return final_df

    finally:
        if tunnel_proc is not None:
            kill_tunnel(tunnel_proc)


def upload_static_variables_DataFrame_with_polars(
    df: pd.DataFrame,
    db_user: str,
    db_password: str,
    column_names: list = ['ticker', 'company_name', 'sedol'],
    value_column_map: dict = {'P_DCOUNTRY': 'primary_domicile_of_country'},
    local_host: bool = False,
    table_name: str = "public.master_table",
    truncate: bool = False,
):
    """
    Static Variables DataFrame을 DB에 Upsert (time 컬럼 없음, PK: sedol).
    load_FactSet_TimeSeriesData로 로드한 MultiIndex DataFrame을 그대로 받아 처리.

    Parameters:
    - df: MultiIndex columns DataFrame (load_FactSet_TimeSeriesData 결과)
    - db_user, db_password: DB 연결 정보
    - column_names: MultiIndex 레벨에 대응하는 DB 컬럼명 리스트
    - local_host: True면 localhost, False면 Cloudflare 터널
    - table_name: 테이블명 (기본값: public.master_table)
    - truncate: True면 TRUNCATE 후 INSERT (전체 교체), False면 UPSERT
    """
    tunnel_proc = None
    try:
        if not local_host:
            tunnel_proc = manage_db_tunnel()
            if tunnel_proc is None:
                print("터널 연결 실패.")
                return None

        port = 5432 if local_host else 15432
        uri = f"postgresql://{db_user}:{quote_plus(db_password)}@127.0.0.1:{port}/quant_data"
        engine = create_engine(uri)

        # MultiIndex DataFrame → flat rows
        flat = pd.DataFrame({
            col_name: df.columns.get_level_values(i)
            for i, col_name in enumerate(column_names)
        })
        for idx_val in df.index:
            col_name = value_column_map.get(str(idx_val), str(idx_val).lower())
            flat[col_name] = df.loc[idx_val].values

        all_cols = list(flat.columns)
        pl_df = pl.from_pandas(flat)
        print(f"Upload 시작: {table_name} ({len(pl_df):,}건)")

        buffer = io.BytesIO()
        pl_df.write_csv(buffer, include_header=False, separator='\t')
        buffer.seek(0)

        raw_name = table_name.split('.')[-1]
        update_cols = ", ".join([f"{col} = EXCLUDED.{col}" for col in all_cols if col != "sedol"])
        cols_str = ", ".join(all_cols)

        conn = engine.raw_connection()
        try:
            with conn.cursor() as cur:
                # 누락 컬럼 자동 추가
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s",
                    (table_name.split('.')[0], raw_name)
                )
                existing_cols = {row[0] for row in cur.fetchall()}
                for col in all_cols:
                    if col not in existing_cols:
                        cur.execute(f'ALTER TABLE {table_name} ADD COLUMN {col} TEXT')
                        print(f"  컬럼 추가: {col} (TEXT)")
                conn.commit()

                if truncate:
                    cur.execute(f"TRUNCATE {table_name}")
                    cur.copy_from(buffer, raw_name, sep="\t", null="", columns=all_cols)
                    conn.commit()
                    print(f"완료! TRUNCATE + {len(pl_df):,}건 INSERT 성공")
                else:
                    cur.execute(f"CREATE TEMP TABLE temp_{raw_name} (LIKE {table_name} INCLUDING DEFAULTS) ON COMMIT DROP")
                    cur.copy_from(buffer, f"temp_{raw_name}", sep="\t", null="", columns=all_cols)
                    cur.execute(f"""
                    INSERT INTO {table_name} ({cols_str})
                    SELECT {cols_str} FROM temp_{raw_name}
                    ON CONFLICT (sedol) DO UPDATE SET {update_cols};
                    """)
                    conn.commit()
                    print(f"완료! {len(pl_df):,}건 Upsert 성공")
        finally:
            conn.close()

    finally:
        if tunnel_proc is not None:
            kill_tunnel(tunnel_proc)


def upload_latest_level_with_polars(
    df: pd.DataFrame,
    db_user: str,
    db_password: str,
    local_host: bool = False,
    table_name: str = "public.adj_latest_level_stock",
    truncate: bool = True,
    conflict_keys: list = ['sedol', 'item_name'],
):
    """
    Flat DataFrame을 DB에 TRUNCATE+INSERT 또는 UPSERT.
    sedol, item_name, latest_level, latest_date 등 이미 완성된 DataFrame을 그대로 업로드.

    Parameters:
    - df: flat DataFrame (컬럼이 DB 테이블 컬럼과 일치)
    - db_user, db_password: DB 연결 정보
    - local_host: True면 localhost, False면 Cloudflare 터널
    - table_name: 테이블명
    - truncate: True면 TRUNCATE 후 INSERT, False면 UPSERT
    - conflict_keys: UPSERT 시 충돌 키 (PK 컬럼)
    """
    tunnel_proc = None
    try:
        if not local_host:
            tunnel_proc = manage_db_tunnel()
            if tunnel_proc is None:
                print("터널 연결 실패.")
                return None

        port = 5432 if local_host else 15432
        uri = f"postgresql://{db_user}:{quote_plus(db_password)}@127.0.0.1:{port}/quant_data"
        engine = create_engine(uri)

        all_cols = list(df.columns)
        pl_df = pl.from_pandas(df)
        print(f"Upload 시작: {table_name} ({len(pl_df):,}건)")

        buffer = io.BytesIO()
        pl_df.write_csv(buffer, include_header=False, separator='\t')
        buffer.seek(0)

        raw_name = table_name.split('.')[-1]
        conflict_str = ", ".join(conflict_keys)
        update_cols = ", ".join([f"{col} = EXCLUDED.{col}" for col in all_cols if col not in conflict_keys])
        cols_str = ", ".join(all_cols)

        conn = engine.raw_connection()
        try:
            with conn.cursor() as cur:
                if truncate:
                    cur.execute(f"TRUNCATE {table_name}")
                    cur.copy_from(buffer, raw_name, sep="\t", null="", columns=all_cols)
                    conn.commit()
                    print(f"완료! TRUNCATE + {len(pl_df):,}건 INSERT 성공")
                else:
                    cur.execute(f"CREATE TEMP TABLE temp_{raw_name} (LIKE {table_name} INCLUDING DEFAULTS) ON COMMIT DROP")
                    cur.copy_from(buffer, f"temp_{raw_name}", sep="\t", null="", columns=all_cols)
                    cur.execute(f"""
                        INSERT INTO {table_name} ({cols_str})
                        SELECT {cols_str} FROM temp_{raw_name}
                        ON CONFLICT ({conflict_str}) DO UPDATE SET {update_cols};
                    """)
                    conn.commit()
                    print(f"완료! {len(pl_df):,}건 Upsert 성공")
        finally:
            conn.close()

    finally:
        if tunnel_proc is not None:
            kill_tunnel(tunnel_proc)


def upload_etf_constituents_DataFrame_with_polars(
    dfs: list,
    universe_names: list,
    db_user: str,
    db_password: str,
    local_host: bool = False,
    table_name: str = "public.monthly_etf_constituents",
):
    """
    Raw sedol DataFrame 리스트를 정규화된 ETF 구성종목 테이블에 Upsert.

    Parameters:
    - dfs: list of wide DataFrames (index=time, values=sedol 문자열)
    - universe_names: list of 유니버스명 (e.g. ["SPY-US", "QQQ-US"])
    """
    if len(dfs) != len(universe_names):
        raise ValueError("dfs와 universe_names의 길이가 같아야 합니다.")

    tunnel_proc = None
    try:
        if not local_host:
            tunnel_proc = manage_db_tunnel()
            if tunnel_proc is None:
                print("🚨 터널 연결 실패.")
                return None

        port = 5432 if local_host else 15432
        uri = f"postgresql://{db_user}:{db_password}@127.0.0.1:{port}/quant_data"
        engine = create_engine(uri)

        print(f"[{_dt.now().strftime('%H:%M:%S')}] 🚀 Upload 시작: {table_name} ({universe_names})")

        # 1. 테이블 확인/생성
        raw_name = table_name.split(".")[-1]
        conn = engine.raw_connection()
        try:
            with conn.cursor() as cur:
                resolved = _resolve_table(cur, table_name)
                if resolved:
                    table_name = resolved
                    print(f"        - 기존 테이블 발견: {table_name}")
                else:
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            time           TIMESTAMPTZ NOT NULL,
                            sedol          TEXT        NOT NULL,
                            universe_name  TEXT        NOT NULL,
                            ticker         TEXT,
                            company_name   TEXT,
                            PRIMARY KEY (time, sedol, universe_name)
                        );
                    """)
                    cur.execute(f"""
                        SELECT EXISTS (
                            SELECT 1 FROM timescaledb_information.hypertables
                            WHERE hypertable_name = '{raw_name}'
                        );
                    """)
                    if not cur.fetchone()[0]:
                        cur.execute(f"SELECT create_hypertable('{table_name}', 'time', if_not_exists => TRUE);")
                        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{raw_name}_sedol ON {table_name} (sedol);")
                        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{raw_name}_universe ON {table_name} (universe_name);")
                    print(f"        - 새 테이블 생성: {table_name}")
                conn.commit()
        finally:
            conn.close()

        # 2. 각 DataFrame → Polars unpivot → concat
        print(f"  [{_dt.now().strftime('%H:%M:%S')}] DataFrame 변환 중...")
        long_parts = []
        for df, uname in zip(dfs, universe_names):
            p_df = pl.from_pandas(df.reset_index().rename(columns={df.index.name or "index": "time"}))
            p_df = p_df.with_columns(pl.col("time").cast(pl.Datetime("us", "UTC")))
            part = (
                p_df.unpivot(index="time", value_name="sedol")
                .drop("variable")
                .filter(pl.col("sedol").is_not_null())
                .with_columns(pl.col("sedol").cast(pl.Utf8))
                .unique(subset=["time", "sedol"])
                .with_columns(pl.lit(uname).alias("universe_name"))
            )
            print(f"        - {uname}: {len(part):,}건")
            long_parts.append(part)

        long_df = pl.concat(long_parts)
        print(f"        - 합계: {len(long_df):,}건")

        # 3. master_table에서 ticker, company_name 매핑 (Polars join)
        print(f"  [{_dt.now().strftime('%H:%M:%S')}] master_table에서 ticker/company_name 매핑 중...")
        master = pl.read_database_uri(
            query="SELECT sedol, ticker, company_name FROM public.master_table", uri=uri
        )
        long_df = long_df.join(master, on="sedol", how="left")
        unmatched = long_df.filter(pl.col("ticker").is_null())
        matched = len(long_df) - len(unmatched)
        print(f"        - 매핑 완료: {matched:,}/{len(long_df):,}건 매칭")
        if len(unmatched) > 0:
            unmatched_sedols = unmatched.select("sedol").unique()["sedol"].to_list()
            print(f"        - 미매칭 sedol ({len(unmatched_sedols)}건): {unmatched_sedols}")

        # 4. COPY → temp → UPSERT
        print(f"  [{_dt.now().strftime('%H:%M:%S')}] DB Upsert 실행 중...")
        final_df = long_df.select(["time", "sedol", "universe_name", "ticker", "company_name"])
        buffer = io.BytesIO()
        final_df.write_csv(buffer, include_header=False, separator='\t')
        buffer.seek(0)

        all_cols = ["time", "sedol", "universe_name", "ticker", "company_name"]
        conn = engine.raw_connection()
        try:
            with conn.cursor() as cur:
                temp_name = f"temp_{raw_name}"
                cur.execute(f"CREATE TEMP TABLE {temp_name} (LIKE {table_name} INCLUDING DEFAULTS) ON COMMIT DROP")
                cur.copy_from(buffer, temp_name, sep="\t", null="", columns=all_cols)

                cols_str = ", ".join(all_cols)
                cur.execute(f"""
                    INSERT INTO {table_name} ({cols_str})
                    SELECT {cols_str} FROM {temp_name}
                    ON CONFLICT (time, sedol, universe_name) DO UPDATE SET
                        ticker = EXCLUDED.ticker,
                        company_name = EXCLUDED.company_name;
                """)
                conn.commit()
                print(f"✅ 완료! {len(final_df):,}건 Upsert 성공 ({_dt.now().strftime('%H:%M:%S')})")
        finally:
            conn.close()

        return final_df

    finally:
        if tunnel_proc is not None:
            kill_tunnel(tunnel_proc)
