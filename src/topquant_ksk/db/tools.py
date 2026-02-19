from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from .tunnel import manage_db_tunnel, kill_tunnel

def check_existing_tables(db_user: str, db_password: str, local_host=False):
    """
    현재 유저가 접근 가능한 모든 테이블 목록과 각 테이블의 컬럼, 행 수, 시간 범위를 출력.

    Parameters:
    - db_user: PostgreSQL 사용자명
    - db_password: PostgreSQL 비밀번호
    - local_host: True이면 터널 없이 localhost 직접 연결, False면 Cloudflare 터널 자동 관리
    """
    tunnel_proc = None
    try:
        if not local_host:
            tunnel_proc = manage_db_tunnel()
            if tunnel_proc is None:
                print("🚨 터널 연결 실패.")
                return

        pw_encoded = quote_plus(db_password)
        uri = f"postgresql://{db_user}:{pw_encoded}@127.0.0.1:5432/quant_data"
        engine = create_engine(uri)

        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT s.table_schema, s.table_name
                FROM (
                    SELECT table_schema, table_name
                    FROM information_schema.tables
                    WHERE table_schema IN ('public', 'private')
                      AND has_schema_privilege(table_schema, 'USAGE')
                ) s
                WHERE has_table_privilege(s.table_schema || '.' || s.table_name, 'SELECT')
                ORDER BY s.table_schema, s.table_name;
            """))
            tables = [(row[0], row[1]) for row in result]
            print(f"\n📋 현재 DB 테이블 목록 ({len(tables)}개):")

            for schema, table in tables:
                full_name = f"{schema}.{table}"
                col_result = conn.execute(text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = :schema AND table_name = :table "
                    "ORDER BY ordinal_position"
                ), {"schema": schema, "table": table})
                all_cols = [row[0] for row in col_result]

                count = conn.execute(text(f"SELECT COUNT(*) FROM {full_name}")).scalar()
                print(f"\n  [{full_name}] ({count:,}건)")

                if 'time' in all_cols:
                    min_time, max_time = conn.execute(text(f"SELECT MIN(time), MAX(time) FROM {full_name}")).fetchone()
                    print(f"    time: {min_time} ~ {max_time}")
                    display_cols = [c for c in all_cols if c != 'time']
                else:
                    display_cols = all_cols

                print(f"    columns: {display_cols}")
    except Exception as e:
        print(f"❌ 에러 발생: {e}")
    finally:
        if tunnel_proc is not None:
            kill_tunnel(tunnel_proc)
