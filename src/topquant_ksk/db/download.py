import pandas as pd
import polars as pl
from datetime import datetime as _dt
from .tunnel import manage_db_tunnel, kill_tunnel


def fetch_timeseries_table(
    table_name: str,
    columns: list,
    item_names: list = None,
    db_user: str = None,
    db_password: str = None,
    local_host: bool = False,
    limit: int = None,
    start_date: str = None,
    end_date: str = None,
    sedols: list | str = "all",
) -> pd.DataFrame:
    """
    DB 시계열 테이블을 조회하여 pandas MultiIndex DataFrame으로 반환.
    PK(time 제외) 기준으로 pivot하고, 나머지 컬럼은 최신값으로 join.

    Parameters:
    - table_name: 테이블명
    - columns: MultiIndex 레벨로 사용할 컬럼명 리스트 (예: ['ticker'] 또는 ['ticker','company_name','sedol'])
    - item_names: 조회할 value 컬럼명 리스트 (None이면 전체)
    - db_user, db_password: DB 연결 정보
    - local_host: True면 터널 없이 localhost 직접 연결, False면 Cloudflare 터널 자동 관리
    - limit: 조회할 행 수 (None이면 전체)
    - start_date: 조회 시작일 (예: '2025-12-31'), None이면 테이블 최소 날짜
    - end_date: 조회 종료일 (예: '2026-01-31'), None이면 테이블 최대 날짜
    - sedols: 조회할 sedol 리스트 또는 단일 문자열, "all"이면 전체 조회

    Returns:
    - pandas DataFrame with MultiIndex columns (item_name, *columns), index=time
    """
    tunnel_proc = None
    try:
        # 터널 관리
        if not local_host:
            tunnel_proc = manage_db_tunnel()
            if tunnel_proc is None:
                print("🚨 터널 연결 실패. 데이터를 가져올 수 없습니다.")
                return None

        port = 5432 if local_host else 15432
        uri = f"postgresql://{db_user}:{db_password}@127.0.0.1:{port}/quant_data"
        print(f"[{_dt.now().strftime('%H:%M:%S')}] 📥 Fetch 시작: {table_name}")

        # 1. 날짜 범위 resolve
        print(f"  [{_dt.now().strftime('%H:%M:%S')}] [1/6] 날짜 범위 확인 중...")
        if start_date is None or end_date is None:
            range_df = pl.read_database_uri(
                query=f"SELECT MIN(time), MAX(time) FROM {table_name}", uri=uri
            )
            db_min, db_max = range_df.row(0)
            if start_date is None:
                start_date = str(db_min.date()) if hasattr(db_min, "date") else str(db_min)[:10]
            if end_date is None:
                end_date = str(db_max.date()) if hasattr(db_max, "date") else str(db_max)[:10]
        print(f"        - 조회 기간: {start_date} ~ {end_date}")

        # WHERE 절 생성
        conditions = [f"time >= '{start_date}'", f"time <= '{end_date} 23:59:59'"]
        if sedols != "all":
            if isinstance(sedols, str):
                sedols = [sedols]
            sedol_list = ", ".join(f"'{s}'" for s in sedols)
            conditions.append(f"sedol IN ({sedol_list})")
            print(f"        - sedol 필터: {len(sedols)}개")

        where_clause = " AND ".join(conditions)
        if item_names:
            select_cols = ", ".join(["time"] + columns + item_names)
        else:
            select_cols = "*"
        query = f"SELECT {select_cols} FROM {table_name} WHERE {where_clause} ORDER BY time, {columns[0]}"
        if limit:
            query += f" LIMIT {limit}"

        # 2. DB 쿼리
        print(f"  [{_dt.now().strftime('%H:%M:%S')}] [2/6] DB 쿼리 실행 중...")
        try:
            df = pl.read_database_uri(query=query, uri=uri)
            print(f"        - 조회 완료: {len(df):,}건")
        except Exception as e:
            print(f"🚨 DB 접속 실패: {e}")
            return None

        # 3. PK 컬럼 자동 조회 (time 제외) - table과 materialized view 모두 지원
        print(f"  [{_dt.now().strftime('%H:%M:%S')}] [3/6] PK 컬럼 조회 중...")
        pk_query = f"""
            SELECT a.attname FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = '{table_name}'::regclass AND i.indisprimary
        """
        pk_df = pl.read_database_uri(query=pk_query, uri=uri)
        pk_all = pk_df["attname"].to_list()
        if not pk_all:
            # Materialized View fallback: 첫 번째 unique index에서 컬럼 조회
            unique_query = f"""
                SELECT a.attname FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indexrelid = (
                    SELECT indexrelid FROM pg_index
                    WHERE indrelid = '{table_name}'::regclass AND indisunique
                    ORDER BY indexrelid LIMIT 1
                )
            """
            pk_df = pl.read_database_uri(query=unique_query, uri=uri)
            pk_all = pk_df["attname"].to_list()
        pk_cols = [c for c in pk_all if c != "time"]
        meta_cols = [c for c in columns if c not in pk_cols]
        print(f"        - PK: {pk_cols}, 메타: {meta_cols}")

        # 4. value 컬럼 식별
        fixed_cols = ["time"] + columns
        value_cols = [c for c in df.columns if c not in fixed_cols]
        if item_names:
            value_cols = [c for c in value_cols if c in item_names]
        print(f"  [{_dt.now().strftime('%H:%M:%S')}] [4/6] value 컬럼: {value_cols}")

        # 5. PK 기준 Unpivot → Pivot
        print(f"  [{_dt.now().strftime('%H:%M:%S')}] [5/6] PK 기준 Pivot 변환 중...")
        pk_info_expr = pl.col(pk_cols[0]).cast(pl.Utf8)
        for c in pk_cols[1:]:
            pk_info_expr = pk_info_expr + "|" + pl.col(c).cast(pl.Utf8)
        df = df.with_columns(pk_info_expr.alias("pk_info"))

        long_df = df.unpivot(
            index=["time", "pk_info"] + pk_cols,
            on=value_cols,
            variable_name="item_name",
            value_name="value",
        )
        long_df = long_df.with_columns(
            (pl.col("item_name") + "|||" + pl.col("pk_info")).alias("col_key")
        )
        wide_df = long_df.pivot(values="value", index="time", on="col_key")

        # 6. MultiIndex 변환 (메타 컬럼은 최신값 join)
        print(f"  [{_dt.now().strftime('%H:%M:%S')}] [6/6] MultiIndex 변환 중...")
        if meta_cols:
            meta_df = df.sort("time").group_by(pk_cols).last().select(pk_cols + meta_cols)
            meta_map = {}
            for row in meta_df.iter_rows(named=True):
                pk_key = "|".join(str(row[c]) for c in pk_cols)
                meta_map[pk_key] = {c: row[c] for c in meta_cols}

        pdf = wide_df.to_pandas().set_index("time")
        new_cols = []
        for col in pdf.columns:
            item_name, pk_info = col.split("|||")
            pk_parts = pk_info.split("|")
            if meta_cols:
                meta_vals = meta_map.get(pk_info, {c: None for c in meta_cols})
                col_tuple = (item_name,) + tuple(meta_vals.get(c, None) for c in meta_cols) + tuple(pk_parts)
            else:
                col_tuple = (item_name,) + tuple(pk_parts)
            new_cols.append(col_tuple)

        level_names = ["item_name"] + meta_cols + pk_cols if meta_cols else ["item_name"] + pk_cols
        pdf.columns = pd.MultiIndex.from_tuples(new_cols, names=level_names)

        print(f"[{_dt.now().strftime('%H:%M:%S')}] ✅ 완료! {pdf.shape[0]:,}행 x {pdf.shape[1]:,}열")
        return pdf

    finally:
        if tunnel_proc is not None:
            kill_tunnel(tunnel_proc)


def fetch_master_table(
    columns: list,
    db_user: str,
    db_password: str,
    local_host: bool = False,
    table_name: str = "public.master_table",
) -> pd.DataFrame:
    """
    마스터 테이블(정적 데이터)을 조회하여 pandas DataFrame으로 반환.
    columns에 지정한 컬럼이 DataFrame의 MultiIndex columns가 되고,
    나머지 컬럼이 값(rows)이 됩니다.

    Parameters:
    - columns: DataFrame columns(MultiIndex)로 사용할 컬럼명 리스트
    - db_user, db_password: DB 연결 정보
    - local_host: True면 localhost, False면 Cloudflare 터널
    - table_name: 테이블명 (기본값: public.master_table)

    Returns:
    - DataFrame: columns=MultiIndex(columns), index=나머지 컬럼명
    """
    tunnel_proc = None
    try:
        if not local_host:
            tunnel_proc = manage_db_tunnel()
            if tunnel_proc is None:
                print("터널 연결 실패.")
                return None

        port = 5432 if local_host else 15432
        uri = f"postgresql://{db_user}:{db_password}@127.0.0.1:{port}/quant_data"
        print(f"[{_dt.now().strftime('%H:%M:%S')}] Fetch: {table_name}")

        df = pl.read_database_uri(query=f"SELECT * FROM {table_name}", uri=uri)
        pdf = df.to_pandas()
        print(f"  조회 완료: {len(pdf):,}건")

        value_cols = [c for c in pdf.columns if c not in columns]
        multi_idx = pd.MultiIndex.from_arrays(
            [pdf[c].values for c in columns], names=columns
        )
        result = pd.DataFrame(
            data=pdf[value_cols].values.T,
            index=value_cols,
            columns=multi_idx,
        )
        result.index.name = None

        print(f"[{_dt.now().strftime('%H:%M:%S')}] 완료! {result.shape[0]}행 x {result.shape[1]:,}열")
        return result

    finally:
        if tunnel_proc is not None:
            kill_tunnel(tunnel_proc)
