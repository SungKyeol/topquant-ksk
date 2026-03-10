from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from .tunnel import manage_db_tunnel, kill_tunnel

def check_existing_tables(db_user: str, db_password: str, local_host=False, detailed_column_date=False):
    """
    현재 유저가 접근 가능한 모든 테이블 목록과 각 테이블의 컬럼, 행 수, 시간 범위를 출력.

    Parameters:
    - db_user: PostgreSQL 사용자명
    - db_password: PostgreSQL 비밀번호
    - local_host: True이면 터널 없이 localhost 직접 연결, False면 Cloudflare 터널 자동 관리
    - detailed_column_date: True이면 각 컬럼별 유효값(non-null)이 존재하는 최대 날짜를 출력
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
        uri = f"postgresql://{db_user}:{pw_encoded}@127.0.0.1:{port}/quant_data"
        engine = create_engine(uri)

        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT s.table_schema, s.table_name, s.table_type
                FROM (
                    SELECT table_schema, table_name, 'TABLE' AS table_type
                    FROM information_schema.tables
                    WHERE table_schema IN ('public', 'private')
                      AND table_type = 'BASE TABLE'
                      AND has_schema_privilege(table_schema, 'USAGE')
                    UNION ALL
                    SELECT schemaname, matviewname, 'MATVIEW'
                    FROM pg_matviews
                    WHERE schemaname IN ('public', 'private')
                ) s
                WHERE has_table_privilege(s.table_schema || '.' || s.table_name, 'SELECT')
                ORDER BY s.table_schema, s.table_name;
            """))
            tables = [(row[0], row[1], row[2]) for row in result]
            print(f"\n📋 현재 DB 테이블 목록 ({len(tables)}개):")

            for schema, table, table_type in tables:
                full_name = f"{schema}.{table}"
                col_result = conn.execute(text("""
                    SELECT a.attname, t.typname
                    FROM pg_attribute a
                    JOIN pg_class c ON a.attrelid = c.oid
                    JOIN pg_namespace n ON c.relnamespace = n.oid
                    JOIN pg_type t ON a.atttypid = t.oid
                    WHERE n.nspname = :schema AND c.relname = :table
                      AND a.attnum > 0 AND NOT a.attisdropped
                    ORDER BY a.attnum
                """), {"schema": schema, "table": table})
                all_cols_info = [(row[0], row[1]) for row in col_result]
                all_cols = [name for name, _ in all_cols_info]
                type_map = {name: typ for name, typ in all_cols_info}

                count = conn.execute(text(f"SELECT COUNT(*) FROM {full_name}")).scalar()
                type_label = "MATVIEW" if table_type == "MATVIEW" else "TABLE"
                print(f"\n  [{full_name}] [{type_label}] ({count:,}건)")

                has_time = 'time' in all_cols
                if has_time:
                    min_time, max_time = conn.execute(text(f"SELECT MIN(time), MAX(time) FROM {full_name}")).fetchone()
                    print(f"    time: {min_time} ~ {max_time}")
                    display_cols = [c for c in all_cols if c != 'time']
                else:
                    display_cols = all_cols

                if detailed_column_date and has_time:
                    col_width = max(len(c) for c in display_cols)
                    type_width = max(len(type_map[c]) for c in display_cols)
                    header = f"    {'column'.ljust(col_width)} | {'type'.ljust(type_width)} | {'date_min':>10} | {'date_max':>10}"
                    separator = f"    {'-' * col_width}-+-{'-' * type_width}-+-{'-' * 10}-+-{'-' * 10}"
                    print(header)
                    print(separator)
                    for col in display_cols:
                        min_date = conn.execute(text(
                            f"SELECT MIN(time) FROM {full_name} WHERE \"{col}\" IS NOT NULL"
                        )).scalar()
                        max_date = conn.execute(text(
                            f"SELECT MAX(time) FROM {full_name} WHERE \"{col}\" IS NOT NULL"
                        )).scalar()
                        min_str = str(min_date)[:10] if min_date else "N/A"
                        max_str = str(max_date)[:10] if max_date else "N/A"
                        print(f"    {col.ljust(col_width)} | {type_map[col].ljust(type_width)} | {min_str:>10} | {max_str:>10}")
                    if 'universe_name' in display_cols:
                        unames = conn.execute(text(
                            f"SELECT DISTINCT universe_name FROM {full_name} ORDER BY universe_name"
                        ))
                        uname_list = [row[0] for row in unames]
                        print(f"    universe_name unique: {uname_list}")
                elif not has_time:
                    col_width = max(len(c) for c in display_cols)
                    type_width = max(len(type_map[c]) for c in display_cols)
                    header = f"    {'column'.ljust(col_width)} | {'type'.ljust(type_width)}"
                    separator = f"    {'-' * col_width}-+-{'-' * type_width}"
                    print(header)
                    print(separator)
                    for col in display_cols:
                        print(f"    {col.ljust(col_width)} | {type_map[col].ljust(type_width)}")
                else:
                    print(f"    columns: {display_cols}")
    except Exception as e:
        print(f"❌ 에러 발생: {e}")
    finally:
        if tunnel_proc is not None:
            kill_tunnel(tunnel_proc)

def compute_cum_PAF(adj_factor, ref_df):
    af = adj_factor.copy()
    if af.index.tz is not None and ref_df.index.tz is None:
        af.index = af.index.tz_localize(None)
    elif af.index.tz is None and ref_df.index.tz is not None:
        af.index = af.index.tz_localize(ref_df.index.tz)
    af = af.reindex(ref_df.index).reindex(ref_df.columns, axis=1)
    return af.shift(-1).fillna(1)[::-1].cumprod()[::-1]