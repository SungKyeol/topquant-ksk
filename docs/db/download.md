# 데이터 조회

## fetch_timeseries_table

시계열 테이블을 조회하여 pandas MultiIndex DataFrame으로 반환합니다.

- **index**: `time` (날짜)
- **columns**: `(item_name, *pk_cols)` MultiIndex

```python
df = conn.download.fetch_timeseries_table(
    table_name="public.daily_adjusted_time_series_data_stock",
    columns=None,              # 자동감지 (ticker, company_name, sedol, index_name 중 존재하는 것)
    item_names=['close_pr', 'close_tr'],  # None이면 전체
    start_date='2020-01-01',   # str 또는 int (0=첫날, -1=마지막날)
    end_date='2025-12-31',
    sedols=['B0YQ5W0'],        # "all"이면 전체
    etf_ticker=['SPY-US'],     # ETF 유니버스 필터
    limit=None,
    save_and_reload_pickle_cache=True,
)
```

### 파라미터

| 파라미터 | 타입 | 기본값 | 설명 |
|----------|------|--------|------|
| `table_name` | str | - | 테이블명 (예: `public.daily_adjusted_time_series_data_stock`) |
| `columns` | list | `None` | MultiIndex 레벨 컬럼. `None`이면 자동감지 |
| `item_names` | list | `None` | 조회할 value 컬럼명. `None`이면 전체 |
| `start_date` | str \| int | `None` | 시작일. 문자열(`'2020-01-01'`) 또는 정수(`0`=첫날, `-1`=마지막날) |
| `end_date` | str \| int | `None` | 종료일. 문자열 또는 정수 |
| `sedols` | list \| str | `"all"` | 조회할 sedol 리스트. `"all"`이면 전체 |
| `etf_ticker` | list \| str \| None | `None` | ETF 유니버스 필터 (`monthly_etf_constituents` 참조) |
| `limit` | int | `None` | 조회 행 수 제한 |
| `save_and_reload_pickle_cache` | bool | `False` | pickle 캐시 사용 여부 |

### Pickle 캐시

`save_and_reload_pickle_cache=True`일 때:

1. **자동 정리**: 당일이 아닌 오래된 캐시 파일을 자동 삭제
2. `pickle_cache/{table_name}_{YYYYMMDD}.pkl` 파일이 존재하면 캐시에서 로드
3. 캐시에 요청한 `item_names`가 모두 있으면 캐시 반환
4. 누락된 항목이 있으면 DB에서 재조회 후 캐시 갱신

!!! note
    캐시 자동 정리는 `fetch_timeseries_table`, `fetch_universe_mask`, `fetch_gics_level_weight` 및 데이터 로딩 함수(`load_FactSet_TimeSeriesData` 등) 모두에 적용됩니다.

---

## fetch_master_table

정적 마스터 테이블을 조회합니다.

- **columns**: 지정한 컬럼이 MultiIndex columns
- **index**: 나머지 컬럼명 (value 컬럼)

```python
df = conn.download.fetch_master_table(
    columns=['ticker', 'company_name', 'sedol'],
    table_name="public.master_table",  # 기본값
)
```

| 파라미터 | 타입 | 기본값 | 설명 |
|----------|------|--------|------|
| `columns` | list | - | MultiIndex로 사용할 컬럼명 리스트 |
| `table_name` | str | `"public.master_table"` | 테이블명 |

---

## fetch_universe_mask

ETF 구성종목 boolean mask를 반환합니다. 리스트 입력 시 합집합(OR).

- **index**: `time`
- **columns**: `(ticker, company_name, sedol)` MultiIndex
- **values**: `bool`

```python
mask = conn.download.fetch_universe_mask(
    etf_ticker=['SPY-US', 'QQQ-US'],
)
```

| 파라미터 | 타입 | 기본값 | 설명 |
|----------|------|--------|------|
| `etf_ticker` | str \| list | - | 유니버스명 (예: `"SPY-US"` 또는 `["SPY-US", "QQQ-US"]`) |
| `table_name` | str | `"public.monthly_etf_constituents"` | 구성종목 테이블명 |
