# DB 도구

## check_existing_tables

현재 유저가 접근 가능한 모든 테이블 목록과 각 테이블의 컬럼, 행 수, 시간 범위를 출력합니다.

```python
conn.tools.check_existing_tables(detailed_column_date=True)
```

| 파라미터 | 타입 | 기본값 | 설명 |
|----------|------|--------|------|
| `detailed_column_date` | bool | `True` | 각 컬럼별 유효값(non-null)이 존재하는 최대 날짜 출력 |

### Viewer User 출력 예시 (2026-03-17 기준)

```
📋 현재 DB 테이블 목록 (6개):

  [public.daily_adjusted_time_series_data_index] [TABLE] (20,503건)
    time: 1999-12-31 ~ 2026-03-16
    ------------------+--------+------------+-----------
    column candidates | type   |   date_min |   date_max
    ------------------+--------+------------+-----------
    ticker            | text   | 1999-12-31 | 2026-03-16
    index_name        | text   | 1999-12-31 | 2026-03-16
    unique index_name: ['NASDAQ 100 Index', 'S&P 500 Equal Weighted', 'SPX Index']
    ------------------+--------+------------+-----------
    item candidates   | type   |   date_min |   date_max
    ------------------+--------+------------+-----------
    open              | float8 | 1999-12-31 | 2026-03-16
    low               | float8 | 1999-12-31 | 2026-03-16
    high              | float8 | 1999-12-31 | 2026-03-16
    close_pr          | float8 | 1999-12-31 | 2026-03-16
    close_tr          | float8 | 1999-12-31 | 2026-03-16

  [public.daily_adjusted_time_series_data_stock] [MATVIEW] (5,884,220건)
    time: 1999-12-31 ~ 2026-03-16
    -----------------------------------------------+--------+------------+-----------
    column candidates                              | type   |   date_min |   date_max
    -----------------------------------------------+--------+------------+-----------
    ticker                                         | text   | 1999-12-31 | 2026-03-16
    company_name                                   | text   | 1999-12-31 | 2026-03-16
    sedol                                          | text   | 1999-12-31 | 2026-03-16
    -----------------------------------------------+--------+------------+-----------
    item candidates                                | type   |   date_min |   date_max
    -----------------------------------------------+--------+------------+-----------
    open                                           | float8 | 1999-12-31 | 2026-03-16
    low                                            | float8 | 1999-12-31 | 2026-03-16
    high                                           | float8 | 1999-12-31 | 2026-03-16
    close_pr                                       | float8 | 1999-12-31 | 2026-03-16
    close_tr                                       | float8 | 1999-12-31 | 2026-03-16
    dps                                            | float8 | 2000-01-03 | 2026-03-16
    forward_next_twelve_months_annual_eps_adjusted | float8 | 1999-12-31 | 2026-03-16
    close_post                                     | float8 | 2011-11-11 | 2026-03-16
    intra_vwap_price                               | float8 | 1999-12-31 | 2026-03-16
    dollar_volume                                  | float8 | 1999-12-31 | 2026-03-16
    marketcap_security                             | float8 | 1999-12-31 | 2026-03-16
    marketcap_company                              | float8 | 1999-12-31 | 2026-03-16
    number_of_estimates_eps                        | int8   | 2019-03-01 | 2026-03-16
    dollar_volume_post                             | float8 | 2011-10-31 | 2026-03-13

  [public.macro_time_series] [TABLE] (47,412건)
    time: 1999-12-31 ~ 2026-03-16
    ------------------+--------+------------+-----------
    column candidates | type   |   date_min |   date_max
    ------------------+--------+------------+-----------
    ticker            | text   | 1999-12-31 | 2026-03-16
    index_name        | text   | 1999-12-31 | 2026-03-16
    unique index_name: ['ICE BofA US Treasury (7-10 Y)', 'ICE BofA US Treasury Bond (1-3 Y)',
                        'US Benchmark Bill - 3 Month', 'US Benchmark Bond - 10 Year',
                        'US Benchmark Bond - 2 Year', 'US Benchmark Bond - 30 Year',
                        'US Benchmark Bond - 5 Year', 'iBoxx USD Liquid Investment Grade Index']
    ------------------+--------+------------+-----------
    item candidates   | type   |   date_min |   date_max
    ------------------+--------+------------+-----------
    ytm               | float8 | 1999-12-31 | 2026-03-16

  [public.master_table] [TABLE] (1,223건)
    ----------------------------+-----
    column candidates           | type
    ----------------------------+-----
    ticker                      | text
    company_name                | text
    sedol                       | text
    ----------------------------+-----
    item candidates             | type
    ----------------------------+-----
    primary_domicile_of_country | text
    delisting_date              | date
    is_inactive                 | bool

  [public.monthly_etf_constituents] [TABLE] (187,038건)
    time: 1999-12-31 ~ 2026-02-28
    ------------------+------+------------+-----------
    column candidates | type |   date_min |   date_max
    ------------------+------+------------+-----------
    ticker            | text | 1999-12-31 | 2026-02-28
    company_name      | text | 1999-12-31 | 2026-02-28
    sedol             | text | 1999-12-31 | 2026-02-28
    ------------------+------+------------+-----------
    item candidates   | type |   date_min |   date_max
    ------------------+------+------------+-----------
    universe_name     | text | 1999-12-31 | 2026-02-28

  [public.monthly_time_series_data_stock] [TABLE] (355,311건)
    time: 1999-12-31 ~ 2026-02-28
    ---------------------------+------+------------+-----------
    column candidates          | type |   date_min |   date_max
    ---------------------------+------+------------+-----------
    ticker                     | text | 1999-12-31 | 2026-02-28
    company_name               | text | 1999-12-31 | 2026-02-28
    sedol                      | text | 1999-12-31 | 2026-02-28
    ---------------------------+------+------------+-----------
    item candidates            | type |   date_min |   date_max
    ---------------------------+------+------------+-----------
    gics_level1_sector         | text | 1999-12-31 | 2026-02-28
    gics_level2_industry_group | text | 1999-12-31 | 2026-02-28
    gics_level3_industry       | text | 1999-12-31 | 2026-02-28
    gics_level4_sub_industry   | text | 1999-12-31 | 2026-02-28
```

---

## compute_cum_PAF

누적 가격 조정 팩터(Price Adjustment Factor)를 계산합니다.

```python
from topquant_ksk.db.tools import compute_cum_PAF

cum_paf = compute_cum_PAF(
    adj_factor=adj_factor_df,  # 조정 팩터 DataFrame
    ref_df=price_df,           # 참조 DataFrame (인덱스/컬럼 기준)
)
```

| 파라미터 | 타입 | 설명 |
|----------|------|------|
| `adj_factor` | DataFrame | 가격 조정 팩터 (index=time, columns=종목) |
| `ref_df` | DataFrame | 참조 DataFrame (reindex 기준) |

timezone 불일치를 자동으로 처리하며, `ref_df`의 인덱스/컬럼에 맞춰 reindex한 뒤 역방향 누적곱을 계산합니다.
