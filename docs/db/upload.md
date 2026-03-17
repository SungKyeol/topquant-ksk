# 데이터 업로드

!!! warning "쓰기 권한 필요"
    Upload 함수들은 **쓰기 권한이 있는 DB 계정**이 필요합니다. 읽기 전용 계정으로는 사용할 수 없습니다.

## upload_stock_timeseries_DataFrame_with_polars

주식 시계열 데이터를 업로드합니다. 여러 DataFrame과 value_names를 매핑하여 UPSERT 또는 TRUNCATE+INSERT 합니다.

```python
conn.upload.upload_stock_timeseries_DataFrame_with_polars(
    dfs=[price_df, return_df],
    value_names=['close_pr', 'close_tr'],
    table_name="public.daily_adjusted_time_series_data_stock",
    truncate=False,  # False=UPSERT, True=TRUNCATE+INSERT
)
```

## upload_index_DataFrame_with_polars

인덱스 시계열 데이터를 업로드합니다.

```python
conn.upload.upload_index_DataFrame_with_polars(
    df=index_df,  # MultiIndex columns: (ticker, index_name, item_name) 또는 (ticker, item_name)
    table_name="adjusted_time_series_data_index",
    truncate=False,
)
```

## upload_index_macro_DataFrame_with_polars

인덱스/매크로 데이터를 커스텀 컬럼 매핑으로 업로드합니다.

```python
conn.upload.upload_index_macro_DataFrame_with_polars(
    df=macro_df,
    col_map={'FG_YIELD': 'ytm'},
    table_name="public.macro_time_series",
)
```

## upload_static_variables_DataFrame_with_polars

정적 변수(master_table 등)를 업로드합니다.

```python
conn.upload.upload_static_variables_DataFrame_with_polars(
    df=master_df,
    column_names=['ticker', 'company_name', 'sedol'],
    value_column_map={'P_DCOUNTRY': 'primary_domicile_of_country'},
    table_name="public.master_table",
)
```

## upload_etf_constituents_DataFrame_with_polars

ETF 구성종목을 업로드합니다.

```python
conn.upload.upload_etf_constituents_DataFrame_with_polars(
    dfs=[spy_df, qqq_df],       # wide format (index=time, values=SEDOL)
    universe_names=['SPY-US', 'QQQ-US'],
)
```

## refresh_materialized_view_concurrently

Materialized View를 리프레시합니다.

```python
conn.upload.refresh_materialized_view_concurrently(
    table_name="daily_adjusted_time_series_data_stock",
    source_tables=['raw_table1', 'raw_table2'],
    join_keys=['sedol', 'time'],
    unique_index_cols=['sedol', 'time'],
)
```

## upload_latest_level_with_polars

이미 평탄화된 DataFrame을 직접 업로드합니다.

```python
conn.upload.upload_latest_level_with_polars(
    df=flat_df,
    table_name="public.adj_latest_level_stock",
    truncate=True,
    conflict_keys=['sedol', 'item_name'],
)
```
