import pandas as pd
import polars as pl
from .tunnel import manage_db_tunnel, kill_tunnel


def fetch_timeseries_table(
    table_name: str,
    columns: list,
    item_names: list = None,
    db_user: str = None,
    db_password: str = None,
    local_host: bool = False,
    limit: int = None,
) -> pd.DataFrame:
    """
    DB 시계열 테이블을 조회하여 pandas MultiIndex DataFrame으로 반환.

    Parameters:
    - table_name: 테이블명
    - columns: MultiIndex 레벨로 사용할 컬럼명 리스트 (예: ['ticker'] 또는 ['ticker','index_name'])
    - item_names: 조회할 value 컬럼명 리스트 (None이면 전체)
    - db_user, db_password: DB 연결 정보
    - local_host: True면 터널 없이 localhost 직접 연결, False면 Cloudflare 터널 자동 관리
    - limit: 조회할 행 수 (None이면 전체)

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

        uri = f"postgresql://{db_user}:{db_password}@127.0.0.1:5432/quant_data"
        print(f"📥 Fetch 시작: {table_name}")

        # 1. DB 쿼리
        print("  [1/4] DB 쿼리 실행 중...")
        query = f"SELECT * FROM {table_name} ORDER BY time, {columns[0]}"
        if limit:
            query += f" LIMIT {limit}"

        try:
            df = pl.read_database_uri(query=query, uri=uri)
            print(f"        - 조회 완료: {len(df):,}건")
        except Exception as e:
            print(f"🚨 DB 접속 실패: {e}")
            return None

        # 2. value 컬럼 식별
        fixed_cols = ["time"] + columns
        value_cols = [c for c in df.columns if c not in fixed_cols]
        if item_names:
            value_cols = [c for c in value_cols if c in item_names]
        print(f"  [2/4] value 컬럼: {value_cols}")

        # 3. Unpivot → Pivot 변환
        print("  [3/4] Unpivot → Pivot 변환 중...")
        info_expr = pl.col(columns[0]).cast(pl.Utf8)
        for c in columns[1:]:
            info_expr = info_expr + "|" + pl.col(c).cast(pl.Utf8)
        df = df.with_columns(info_expr.alias("info"))

        long_df = df.unpivot(
            index=["time", "info"],
            on=value_cols,
            variable_name="item_name",
            value_name="value",
        )
        long_df = long_df.with_columns(
            (pl.col("item_name") + "|||" + pl.col("info")).alias("col_key")
        )
        wide_df = long_df.pivot(values="value", index="time", on="col_key")

        # 4. MultiIndex 변환
        print("  [4/4] MultiIndex 변환 중...")
        pdf = wide_df.to_pandas().set_index("time")
        new_cols = []
        for col in pdf.columns:
            item_name, info = col.split("|||")
            parts = info.split("|")
            new_cols.append((item_name,) + tuple(parts))
        pdf.columns = pd.MultiIndex.from_tuples(new_cols, names=["item_name"] + columns)

        print(f"✅ 완료! {pdf.shape[0]:,}행 x {pdf.shape[1]:,}열")
        return pdf

    finally:
        if tunnel_proc is not None:
            kill_tunnel(tunnel_proc)
