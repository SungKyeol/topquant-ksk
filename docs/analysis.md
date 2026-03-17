# 위험/수익 분석

## get_RiskReturnProfile

종합 위험/수익 지표를 산출합니다.

```python
from topquant_ksk import get_RiskReturnProfile

profile = get_RiskReturnProfile(
    rebalencing_ret=strategy_daily_returns,           # DataFrame (여러 전략 가능)
    cash_return_daily_BenchmarkFrequency=rf_daily,    # 무위험수익률 Series
    BM_ret=benchmark_daily_returns,                   # Optional: 벤치마크 Series
)
```

### 반환 지표

**기본 지표:**

| 지표 | 설명 |
|------|------|
| CAGR | 연평균 복리 수익률 |
| STD | 연환산 표준편차 |
| Sharpe | 샤프 비율 |
| MDD | 최대 낙폭 |
| MDD 시점 | 최대 낙폭 발생 시점 |
| UnderWaterPeriod | 수중 기간 |
| 1M/3M/6M/1Y/3Y Ret | 기간별 수익률 |

**벤치마크(`BM_ret`) 제공 시 추가:**

| 지표 | 설명 |
|------|------|
| Excess Return | 초과수익률 |
| Tracking Error | 추적 오차 |
| IR | 정보 비율 |
| 주간 승률 | 주간 초과수익 승률 |
| 최대 상대 손실 | 벤치마크 대비 최대 손실 |

---

## get_yearly_monthly_ER

연도별/월별 초과수익을 계산합니다.

```python
from topquant_ksk import get_yearly_monthly_ER

er_df = get_yearly_monthly_ER(strategy_return, BM_return)

# YearlyMonthlyERDataFrame 전용 히트맵 메서드
er_df.heatmap()
```

`get_yearly_monthly_ER`은 `YearlyMonthlyERDataFrame` 타입을 반환하며, `.heatmap()` 메서드로 연도-월 히트맵을 바로 시각화할 수 있습니다.
