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

리밸런싱 목표비중으로부터 일일 포트폴리오 수익률, 비중, 턴오버를 계산합니다.

```python
from topquant_ksk import compute_daily_weights_rets_from_rebal_targets

port_ret, daily_weights, turnover = compute_daily_weights_rets_from_rebal_targets(
    target_weights_at_rebal_time=target_weights,  # 리밸런싱일 x 종목 비중
    price_return_daily=price_ret,                  # 일별 가격수익률
    total_return_daily=total_ret,                   # 일별 총수익률
    transaction_cost_rate=0.001,                    # 거래비용 0.1%
)
```

### 반환값

| 반환 | 타입 | 설명 |
|------|------|------|
| `port_ret` | Series | 일별 포트폴리오 수익률 |
| `daily_weights` | DataFrame | 일별 종목 비중 |
| `turnover` | Series | 리밸런싱일 턴오버 |
