import os
from topquant_ksk.db import DBConnection

# ── 1. DB 연결 ──
conn = DBConnection(
    db_user=os.environ['DB_USER'],
    db_password=os.environ['DB_PASSWORD'],
)

# ── 2. 테이블 목록 확인 ──
conn.tools.check_existing_tables()

# ── 3. S&P500 종목 주가 조회 (최근 10년) ──
stock = conn.download.fetch_timeseries_table(
    table_name="public.daily_adjusted_time_series_data_stock",
    item_names=['close_pr', 'close_tr','dollar_volume'],
    start_date='2016-01-01',
    etf_ticker='SPY-US',
    save_and_reload_pickle_cache=True,
)

stock['dollar_volume']['NVDA']
