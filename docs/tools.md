# 유틸리티

## cash_return_trading_date

YTM(연%) 시리즈를 거래일 기준 일일수익률로 변환합니다.

```python
from topquant_ksk import cash_return_trading_date

rf_daily = cash_return_trading_date(
    rf_ytm=ytm_series,                # YTM(연%) Series
    trading_date_index=price.index,    # 거래일 DatetimeIndex
)
```

---

## resample_last_date

일별 데이터를 월말/분기말/연말 마지막 값으로 리샘플링합니다.

```python
from topquant_ksk import resample_last_date

monthly = resample_last_date(daily_data, freq='M')
quarterly = resample_last_date(daily_data, freq='Q')
yearly = resample_last_date(daily_data, freq='Y')
```

| 파라미터 | 타입 | 설명 |
|----------|------|------|
| `data` | DataFrame | 일별 데이터 |
| `freq` | str | 리샘플링 주기 (`'M'`, `'Q'`, `'Y'`) |

---

## compute_daily_weights_rets_from_rebal_targets

리밸런싱 목표비중으로부터 일일 포트폴리오 수익률, 비중, 턴오버를 계산합니다. `entry_lag`와 `entry_price`로 진입 시점/가격을 지정할 수 있습니다.

```python
from topquant_ksk import compute_daily_weights_rets_from_rebal_targets

after_cost, before_cost, daily_weights, turnover = compute_daily_weights_rets_from_rebal_targets(
    target_weights_at_rebal_time=target_weights,  # 리밸런싱일 x 종목 비중
    price_return_daily=price_ret,                  # 일별 가격수익률
    total_return_daily=total_ret,                   # 일별 총수익률
    transaction_cost_rate=0.001,                    # 거래비용 0.1%
    close_price=close_df,                           # Optional: 종가 DataFrame
    entry_price=open_df,                            # Optional: 진입가격 DataFrame
    entry_lag=1,                                    # Optional: 진입 지연 (거래일 수)
)
```

### 파라미터

| 파라미터 | 타입 | 기본값 | 설명 |
|----------|------|--------|------|
| `target_weights_at_rebal_time` | DataFrame | - | 리밸런싱일 x 종목 비중 |
| `price_return_daily` | DataFrame | - | 일별 가격수익률 |
| `total_return_daily` | DataFrame | - | 일별 총수익률 |
| `transaction_cost_rate` | float | - | 거래비용 비율 |
| `close_price` | DataFrame | `None` | 종가. 제공 시 entry 로직 활성화 |
| `entry_price` | DataFrame | `None` | 진입가격 (예: 시가) |
| `entry_lag` | int | `0` | 신호일 이후 진입까지 지연 거래일 수 |

### 반환값

| 반환 | 타입 | 설명 |
|------|------|------|
| `after_cost` | Series | 거래비용 차감 후 일별 포트폴리오 수익률 |
| `before_cost` | Series | 거래비용 차감 전 일별 포트폴리오 수익률 |
| `daily_weights` | DataFrame | 일별 종목 비중 (EOD) |
| `turnover` | Series | 리밸런싱일 턴오버 |

### 진입 모드

| 모드 | 설정 | 설명 |
|------|------|------|
| 당일 종가 진입 | 기본값 (close_price=None) | close-to-close drift |
| 당일 종가 진입 (명시적) | close_price=close, entry_lag=0 | 동일하지만 명시적 |
| T+1 시가 진입 | close_price=close, entry_price=open, entry_lag=1 | 신호 다음날 시가로 진입 |

---

## quantile

DataFrame의 각 행(또는 열)에 대해 분위수 라벨(1~q)을 부여합니다.

```python
from topquant_ksk import quantile

labels = quantile(dataframe, q=5, axis=1)  # 5분위, 행 기준
```

| 파라미터 | 타입 | 기본값 | 설명 |
|----------|------|--------|------|
| `dataframe` | DataFrame | - | 분위수를 구할 데이터 |
| `q` | int | - | 분위수 개수 (예: 5 → 5분위) |
| `axis` | int | `1` | 1=행 기준, 0=열 기준 |

---

## quantile_return_by_group

분위수별 평균 수익률을 산출합니다. `quantile()` 함수의 출력과 수익률 DataFrame을 입력받습니다.

```python
from topquant_ksk import quantile_return_by_group

q_ret = quantile_return_by_group(
    quantile_df=labels,    # quantile() 출력 (1~q 라벨)
    return_df=daily_ret,   # 수익률 DataFrame
)
# q_ret.columns = [1, 2, 3, 4, 5]  (분위수 번호)
```

---

## cagr

구간 수익률 DataFrame/Series로부터 연환산 수익률(CAGR)을 계산합니다.

```python
from topquant_ksk import cagr

annual_return = cagr(return_df)  # Series 반환 (columns별 CAGR)
```

---

## annualized_turnover

턴오버를 연율화합니다.

```python
from topquant_ksk import annualized_turnover

ann_to = annualized_turnover(turnover_series, skip_first=True)
```

| 파라미터 | 타입 | 기본값 | 설명 |
|----------|------|--------|------|
| `turnover` | Series \| DataFrame | - | 리밸런싱 시점별 턴오버 |
| `skip_first` | bool | `True` | 첫 번째 값(초기 진입) 제외 여부 |

---

## rounding_target_weight

포트폴리오 비중을 반올림한 후 합계가 100%가 되도록 보정합니다. Active weight 크기 순으로 round-robin 방식으로 오차를 배분합니다.

```python
from topquant_ksk import rounding_target_weight

rounded_weights = rounding_target_weight(
    target_weight=raw_weights,  # 리밸런싱 비중 (full precision)
    bm=bm_weights,              # 벤치마크 비중 (daily)
    n_round=3,                  # 소수점 자릿수 (기본 3 → 0.1% 단위)
)
```

| 파라미터 | 타입 | 기본값 | 설명 |
|----------|------|--------|------|
| `target_weight` | DataFrame | - | 리밸런싱 비중 (index=날짜, columns=종목) |
| `bm` | DataFrame | - | 벤치마크 비중 (daily, 내부에서 reindex) |
| `n_round` | int | `3` | 소수점 자릿수. step = 10^(-n_round) |
