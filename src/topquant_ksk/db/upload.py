import io
import os
import time
import xlwings as xw
import pandas as pd
import polars as pl
from datetime import datetime as _dt
from sqlalchemy import create_engine
from .tunnel import manage_db_tunnel, kill_tunnel


def _resolve_table(cur, table_name):
    """테이블이 이미 존재하면 schema.table_name 반환, 없으면 None. 여러 스키마에 존재하면 오류."""
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


def run_factset_refresh_N_save_to_csv(file_path):
    _ts = lambda: _dt.now().strftime('%H:%M:%S')

    # 1. FactSet Fix Excel 실행 (Add-in 안정화)
    FIXEXCEL_PATH = r"C:\Program Files (x86)\FactSet\fdswFixExcel.exe"
    if os.path.exists(FIXEXCEL_PATH):
        print(f"[{_ts()}] 🔧 1. FactSet Fix Excel 실행 중...")
        os.startfile(FIXEXCEL_PATH)
        time.sleep(3)

    file_name = os.path.basename(file_path)
    print(f"[{_ts()}] 🚀 2. {file_name} 실행 및 로드 대기...")
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
                    # 수리도구가 남긴 빈 문서 정리
                    for b in active_app.books:
                        if b.name.startswith("Book") and b.name != file_name:
                            b.close()
                    break
            if wb: break
        except: pass
        time.sleep(1)

    if not wb: raise TimeoutError(f"[{_ts()}] 🔥 엑셀 파일 연결에 실패했습니다.")

    try:
        # 3. FactSet 공식 매크로 호출 (전체 재계산)
        print(f"[{_ts()}] ⚡ 3. FactSet 전체 재계산 실행 (FDS_RECALC_NOW)...")
        time.sleep(1) # Add-in 로딩 보장

        app.activate(steal_focus=True)
        # 공식 매크로 호출
        app.api.Run("FDS_RECALC_NOW")

        # 4. 데이터 확정 모니터링 (7번째 행 감시)
        print(f"[{_ts()}] ⏳ 4. 데이터 계산 완료 확인 중 (#Calc 탈출 감시)...")
        sheet = wb.sheets[-1]
        for i in range(180):
            # 7행 전체 스캔 (A7:CZ7)
            row_vals = sheet.range("A7:CZ7").value
            str_vals = [str(v).strip() for v in row_vals if v is not None]

            # #Calc가 없고, 유효한 값(숫자 또는 #N/A)이 존재할 때 탈출
            if all("#Calc" not in v for v in str_vals) and len(str_vals) > 0:
                print(f"[{_ts()}] 🎉 데이터 갱신 확인 완료! (소요 시간: {i}초)")
                break
            time.sleep(1)
        else:
            print(f"[{_ts()}] ⏰ 경고: 계산 대기 시간이 초과되었습니다.")

        # 5. PERSONAL.XLSB 스마트 카피 실행
        print(f"[{_ts()}] 🚀 5. 스마트 카피 매크로 실행 (CSV 내보내기)...")
        app.api.Run("PERSONAL.XLSB!Export_FactSet_Smart_Copy")

        # 6. 프로세스 종료
        wb.close()
        print(f"[{_ts()}] 💾 6. 작업 파일 닫기 및 모든 자동화 프로세스 종료.")

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

        uri = f"postgresql://{db_user}:{db_password}@127.0.0.1:5432/quant_data"
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
                resolved = _resolve_table(cur, raw_name)
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


def upload_stock_timeseries_DataFrame_with_polars(dfs: list, 
                                                  value_names: list,
                                                  table_name: str,                                                    
                                                  db_user: str, 
                                                  db_password: str, 
                                                  local_host=False, 
                                                  truncate=False):
    """
    Stock DataFrame 리스트를 DB에 Upsert.
    테이블이 없으면 자동 생성.

    Parameters:
    - dfs: List of pandas DataFrames (MultiIndex columns: ticker, company_name, sedol)
    - value_names: List of column names for values (e.g. ["close_pr", "close_tr"])
    - db_user: PostgreSQL 사용자명
    - db_password: PostgreSQL 비밀번호
    - local_host: True이면 터널 없이 localhost 직접 연결
    - table_name: Target table name
    - truncate: True이면 insert 전에 테이블을 TRUNCATE
    """
    if len(dfs) != len(value_names):
        raise ValueError("dfs와 value_names의 길이가 같아야 합니다.")

    tunnel_proc = None
    try:
        if not local_host:
            tunnel_proc = manage_db_tunnel()
            if tunnel_proc is None:
                print("🚨 터널 연결 실패.")
                return None

        uri = f"postgresql://{db_user}:{db_password}@127.0.0.1:5432/quant_data"
        engine = create_engine(uri)

        print(f"[{_dt.now().strftime('%H:%M:%S')}] 🚀 Upload 시작: {table_name} 테이블에 {value_names} 컬럼 업로드")

        # 1. 기존 테이블 확인 / 없으면 생성
        print(f"  [{_dt.now().strftime('%H:%M:%S')}] [1/5] 테이블 확인/생성 중...")
        raw_name = table_name.split(".")[-1]
        value_cols_sql = ",\n                        ".join([f"{col} DOUBLE PRECISION" for col in value_names])
        conn = engine.raw_connection()
        try:
            with conn.cursor() as cur:
                resolved = _resolve_table(cur, raw_name)
                if resolved:
                    table_name = resolved
                    print(f"        - 기존 테이블 발견: {table_name}")
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
        for df, value_name in zip(dfs, value_names):
            df_copy = df.copy()
            df_copy.columns = [f"{ticker}|{company_name}|{sedol}" for ticker, company_name, sedol in df_copy.columns]
            p_df = pl.from_pandas(df_copy.reset_index().rename(columns={df_copy.index.name or "index": "time"}))
            p_df = p_df.with_columns(pl.col("time").cast(pl.Datetime("us", "UTC")))
            p_long = p_df.unpivot(index="time", variable_name="info", value_name=value_name)
            p_long = p_long.filter(pl.col(value_name).is_not_null())
            polars_longs.append(p_long)
            print(f"        - {value_name}: {len(p_long):,}건")

        # 3. Concat + Pivot 방식으로 병합
        print(f"  [{_dt.now().strftime('%H:%M:%S')}] [3/5] Concat + Pivot 병합 중...")
        dfs_with_type = []
        for p_long, value_name in zip(polars_longs, value_names):
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
            ["time", "ticker", "company_name", "sedol"] + value_names
        )
        print(f"        - 최종 데이터: {len(final_df):,}건")

        # 5. CSV 버퍼 → COPY → UPSERT
        print(f"  [5/5] DB {'Truncate + Insert' if truncate else 'Upsert'} 실행 중... ({_dt.now().strftime('%H:%M:%S')})")
        buffer = io.BytesIO()
        final_df.write_csv(buffer, include_header=False, separator='\t')
        buffer.seek(0)

        all_cols = ["time", "ticker", "company_name", "sedol"] + value_names
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

                    update_cols = ", ".join([f"{col} = EXCLUDED.{col}" for col in value_names + ["ticker", "company_name"]])
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
