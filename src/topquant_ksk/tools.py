import pandas as pd

def resample_last_date(data_daily, freq='M'):
    monthly_periods = data_daily.index.to_period(freq)
    data_monthly = data_daily.groupby(monthly_periods, group_keys=False).apply(lambda x: x.iloc[[-1]])
    return data_monthly

def compute_daily_weights_rets_from_rebal_targets(target_weights_at_rebal_time: pd.DataFrame, price_return_daily: pd.DataFrame, total_return_daily: pd.DataFrame,transaction_cost_rate) -> pd.DataFrame:
    """리밸런싱 타깃 비중(End-of-Day, EOD)과 intra-period 누적 수익률로 일별 실제 End-of-Day 비중을 계산한다.
    - 리밸런싱일의 타깃 비중은 해당 일자의 EOD 비중으로 유지
    - 리밸런싱 구간 내에서는 다음 날부터 현재일까지의 누적 수익률로 비중이 드리프트되어 EOD 비중이 됨
    - 일별 포트 수익률 계산 시에는 EOD 비중을 하루 쉬프트하여 SOD 비중으로 사용
    """
    price_return_daily=price_return_daily[target_weights_at_rebal_time.index[0]:]
    total_return_daily=total_return_daily[target_weights_at_rebal_time.index[0]:]
    all_days = price_return_daily.index
    rebal_dates = target_weights_at_rebal_time.index
    # 각 날짜별 속한 리밸런싱 시작일(이전 리밸런싱일) 라벨 생성
    anchor_series = pd.Series(rebal_dates, index=rebal_dates).reindex(all_days, method='ffill')

    # 그룹별 누적 수익률(리밸런싱일 포함)과 그룹 첫날(리밸런싱일)의 (1+r) 값 추출
    ret_plus_one = price_return_daily + 1.0
    cumprod_to_date = ret_plus_one.groupby(anchor_series).cumprod()
    cumprod_to_date_intra_period_groupby=cumprod_to_date.groupby(anchor_series)
    first_day_factor = cumprod_to_date_intra_period_groupby.transform('first') #리벨런싱일 ret+1을 group에 브로드캐스트

    #turnover
    intra_period_return_rebal_freq_strat_of_period=cumprod_to_date_intra_period_groupby.last() #리밸런싱 기간내 수익률, index 리밸런싱 시작점
    end_of_period_weights_unnormalized=target_weights_at_rebal_time*intra_period_return_rebal_freq_strat_of_period
    end_of_period_weights=end_of_period_weights_unnormalized.div(end_of_period_weights_unnormalized.sum(axis=1), axis=0)
    turnover_by_stock=(target_weights_at_rebal_time.fillna(0)-end_of_period_weights.shift(1).fillna(0)).abs()
    portfolio_turnover_series=turnover_by_stock.sum(axis=1)

    # 리밸런싱일의 EOD 비중이 타깃이 되도록, (리밸런싱일+1)부터의 누적수익률만 반영
    # cumprod_excluding_rebal_day: d0(리밸런싱일)에서는 1, d0+1에서는 (1+r_{d0+1}), ...
    cumprod_excluding_rebal_day = (cumprod_to_date / first_day_factor).fillna(1.0)

    # 타깃 비중을 일별로 확장해 매핑
    tw_daily = target_weights_at_rebal_time.reindex(anchor_series.values).set_index(all_days)

    # 비정규화 일별 가치 및 정규화된 실제 EOD 비중
    unnormalized_values = tw_daily * cumprod_excluding_rebal_day
    daily_eod_weights = unnormalized_values.div(unnormalized_values.sum(axis=1), axis=0).fillna(0)
    daily_eod_weights=daily_eod_weights[rebal_dates[0]:]

    pfl_return_series = (daily_eod_weights.shift(1) * total_return_daily).sum(axis=1).dropna()
    transaction_cost_at_rebal_time = portfolio_turnover_series * -transaction_cost_rate
    indexer = pfl_return_series.index.get_indexer(transaction_cost_at_rebal_time.index, method='ffill') #rebalancing date 위치
    transaction_cost_at_rebal_time.index = pfl_return_series.index[indexer]
    pfl_return_series_after_cost_daily = pfl_return_series + transaction_cost_at_rebal_time.reindex(pfl_return_series.index).fillna(0)

    zero_weight=(daily_eod_weights.sum(axis=1)==0)
    pfl_return_series_after_cost_daily[zero_weight]=np.nan

    return pfl_return_series_after_cost_daily, daily_eod_weights, portfolio_turnover_series    