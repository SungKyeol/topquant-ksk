---
name: topquant-ksk
description: topquant_ksk 라이브러리 API 레퍼런스. 코드에서 topquant_ksk를 import하거나 DBConnection, fetch_timeseries_table, get_RiskReturnProfile 등을 사용할 때 자동 참조.
user-invocable: false
---

# topquant-ksk API Reference

## 설치
```bash
pip install topquant-ksk          # 기본
pip install topquant-ksk[db]      # DB 기능 포함
pip install topquant-ksk[plot]    # 시각화 포함
pip install topquant-ksk[all]     # 전체
```

## 패키지 구조
```
topquant_ksk/
├── load_data.py          # FactSet/DataGuide 파일 로딩
├── tools.py              # 유틸리티 (수익률 계산, 리샘플링, 포트폴리오)
├── risk_return_metrics.py # 위험/수익 지표
├── plot.py               # 히트맵 시각화
└── db/
    ├── connection.py     # DBConnection 클래스
    ├── download.py       # DB fetch 함수
    ├── upload.py         # DB upload 함수
    ├── tools.py          # DB 유틸리티
    └── tunnel.py         # Cloudflare 터널
```

---

## 1. DB 모듈

### DBConnection
```python
from topquant_ksk.db import DBConnection
conn = DBConnection(db_user="user", db_password="pw", local_host=False)
# conn.download.fetch_timeseries_table(...)
# conn.upload.upload_stock_timeseries_DataFrame_with_polars(...)
# conn.tools.check_existing_tables(...)
```

### fetch_timeseries_table
시계열 테이블 → pandas MultiIndex DataFrame (index=time, columns=(item_name, *pk_cols))
```python
df = conn.download.fetch_timeseries_table(
    table_name="public.daily_adjusted_time_series_data_stock",
    columns=None,              # 자동감지 (ticker, company_name, sedol, index_name 중 존재하는 것)
    item_names=['close_pr', 'close_tr'],  # None이면 전체
    start_date='2020-01-01',   # str 또는 int (0=첫날, -1=마지막날)
    end_date='2025-12-31',
    sedols=['B0YQ5W0'],        # "all"이면 전체
    etf_ticker=['SPY-US'],     # ETF 유니버스 필터 (monthly_etf_constituents 참조)
    limit=None,
    save_and_reload_pickle_cache=True,  # pickle_cache/{table}_{YYYYMMDD}.pkl
)
```

### fetch_master_table
정적 마스터 테이블 조회 → DataFrame (columns=MultiIndex, index=value컬럼명)
```python
df = conn.download.fetch_master_table(
    columns=['ticker', 'company_name', 'sedol'],
    table_name="public.master_table",
)
```

### fetch_universe_mask
ETF 구성종목 boolean mask → DataFrame (index=time, columns=(ticker, company_name, sedol), values=bool)
```python
mask = conn.download.fetch_universe_mask(
    etf_ticker=['SPY-US', 'QQQ-US'],  # 합집합(OR)
)
```

### Upload 함수들 (쓰기 권한이 있는 DB 계정 필요)
```python
# 주식 시계열 업로드 (여러 DataFrame + value_names 매핑)
conn.upload.upload_stock_timeseries_DataFrame_with_polars(
    dfs=[price_df, return_df],
    value_names=['close_pr', 'close_tr'],
    table_name="public.daily_adjusted_time_series_data_stock",
    truncate=False,  # False=UPSERT, True=TRUNCATE+INSERT
)

# 인덱스 시계열 업로드
conn.upload.upload_index_DataFrame_with_polars(
    df=index_df,  # MultiIndex columns: (ticker, index_name, item_name) 또는 (ticker, item_name)
    table_name="adjusted_time_series_data_index",
    truncate=False,
)

# 인덱스/매크로 커스텀 컬럼 매핑 업로드
conn.upload.upload_index_macro_DataFrame_with_polars(
    df=macro_df,
    col_map={'FG_YIELD': 'ytm'},
    table_name="public.macro_time_series",
)

# 정적 변수 업로드 (master_table 등)
conn.upload.upload_static_variables_DataFrame_with_polars(
    df=master_df,
    column_names=['ticker', 'company_name', 'sedol'],
    value_column_map={'P_DCOUNTRY': 'primary_domicile_of_country'},
    table_name="public.master_table",
)

# ETF 구성종목 업로드
conn.upload.upload_etf_constituents_DataFrame_with_polars(
    dfs=[spy_df, qqq_df],       # wide format (index=time, values=SEDOL)
    universe_names=['SPY-US', 'QQQ-US'],
)

# Materialized View 리프레시
conn.upload.refresh_materialized_view_concurrently(
    table_name="daily_adjusted_time_series_data_stock",
    source_tables=['raw_table1', 'raw_table2'],
    join_keys=['sedol', 'time'],
    unique_index_cols=['sedol', 'time'],
)

# 이미 평탄화된 DataFrame 직접 업로드
conn.upload.upload_latest_level_with_polars(
    df=flat_df,
    table_name="public.adj_latest_level_stock",
    truncate=True,
    conflict_keys=['sedol', 'item_name'],
)
```

### DB Tools
```python
# 전체 테이블 목록 + 컬럼/행수/날짜범위 출력
conn.tools.check_existing_tables(detailed_column_date=True)
```

---

## 2. 데이터 로딩

```python
from topquant_ksk import load_FactSet_TimeSeriesData, load_DataGuide_TimeSeriesData

# FactSet 시계열 (Excel/CSV)
df = load_FactSet_TimeSeriesData(
    filename="data.xlsx",
    column_spec=['ticker', 'item_name'],  # MultiIndex 레벨
    sheet_name='TimeSeries',
    encoding='utf-8',
    dropna_cols=False,
    type_conversion='float',  # 'float', 'str', None
)

# DataGuide 시계열 (3레벨 MultiIndex)
df = load_DataGuide_TimeSeriesData(
    filename="dg_data.xlsx",
    column_spec=['Item Name', 'Symbol Name', 'Symbol'],
)

# DataGuide 인덱스 (2레벨), 경제 (1레벨), 횡단면
load_DataGuide_IndexData(filename, column_spec=['Item Name', 'Symbol Name'])
load_DataGuide_EconomicData(filename, column_spec=['Item Name'])
load_DataGuide_CrossSectionalData(filename, encoding='utf-8')
```

---

## 3. 위험/수익 분석

```python
from topquant_ksk import get_RiskReturnProfile, get_yearly_monthly_ER

# 종합 위험/수익 지표
profile = get_RiskReturnProfile(
    rebalencing_ret=strategy_daily_returns,           # DataFrame (여러 전략 가능)
    cash_return_daily_BenchmarkFrequency=rf_daily,    # 무위험수익률 Series
    BM_ret=benchmark_daily_returns,                   # Optional: 벤치마크 Series
)
# 반환 지표: CAGR, STD, Sharpe, MDD, MDD시점, UnderWaterPeriod, 1M/3M/6M/1Y/3Y Ret
# BM 제공 시 추가: excess_return, tracking_error, IR, 주간승률, 최대상대손실

# 연도별/월별 초과수익
er_df = get_yearly_monthly_ER(strategy_return, BM_return)
er_df.heatmap()  # YearlyMonthlyERDataFrame 전용 메서드
```

---

## 4. 유틸리티 (tools)

```python
from topquant_ksk import cash_return_trading_date, resample_last_date
from topquant_ksk import compute_daily_weights_rets_from_rebal_targets

# YTM(연%) → 거래일 일일수익률
rf_daily = cash_return_trading_date(rf_ytm=ytm_series, trading_date_index=price.index)

# 리샘플링 (월말/분기말/연말 마지막 값)
monthly = resample_last_date(daily_data, freq='M')

# 리밸런싱 목표비중 → 일일 포트폴리오 수익률/비중/턴오버
port_ret, daily_weights, turnover = compute_daily_weights_rets_from_rebal_targets(
    target_weights_at_rebal_time=target_weights,  # 리밸런싱일 x 종목 비중
    price_return_daily=price_ret,
    total_return_daily=total_ret,
    transaction_cost_rate=0.001,  # 0.1%
)
```

---

## 5. 시각화

```python
from topquant_ksk import heatmap

heatmap(dataframe, size=(12,6), annot=True, vmax=None, vmin=None,
        title=None, rotation=0, fontsize=25, show_colorbar=False)
# RdYlBu_r 컬러맵, 값 자동 % 변환
```
