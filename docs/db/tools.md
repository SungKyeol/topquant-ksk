# DB 도구

## check_existing_tables

현재 유저가 접근 가능한 모든 테이블 목록과 각 테이블의 컬럼, 행 수, 시간 범위를 출력합니다.

```python
conn.tools.check_existing_tables(detailed_column_date=True)
```

| 파라미터 | 타입 | 기본값 | 설명 |
|----------|------|--------|------|
| `detailed_column_date` | bool | `True` | 각 컬럼별 유효값(non-null)이 존재하는 최대 날짜 출력 |

### 출력 예시

```
📋 현재 DB 테이블 목록 (5개):

  [public.daily_adjusted_time_series_data_stock] [MATVIEW] (1,234,567건)
    time: 2000-01-03 ~ 2025-12-31
    -----------------+---------+------------+------------
    column candidates | type    |   date_min |   date_max
    -----------------+---------+------------+------------
    ticker           | varchar | 2000-01-03 | 2025-12-31
    company_name     | varchar | 2000-01-03 | 2025-12-31
    sedol            | varchar | 2000-01-03 | 2025-12-31
    -----------------+---------+------------+------------
    item candidates  | type    |   date_min |   date_max
    -----------------+---------+------------+------------
    close_pr         | float8  | 2000-01-03 | 2025-12-31
    close_tr         | float8  | 2000-01-03 | 2025-12-31
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
