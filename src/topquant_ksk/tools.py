import pandas as pd
import numpy as np

def cash_return_trading_date(rf_ytm: pd.Series, trading_date_index: pd.DatetimeIndex) -> pd.Series:
    """무위험수익률(YTM, %) → 트레이딩일 기준 일별 현금 수익률로 변환.

    1. YTM을 일별 수익률로 변환 (/ 100 / 365)
    2. 주말 포함 calendar daily 인덱스로 확장 후 ffill
    3. 누적 수익률 계산
    4. 트레이딩일 인덱스로 reindex 후 pct_change 반환
    """
    # Normalize timezones: strip tz if present for compatibility
    if rf_ytm.index.tz is not None:
        rf_ytm = rf_ytm.copy()
        rf_ytm.index = rf_ytm.index.tz_localize(None)
    if trading_date_index.tz is not None:
        trading_date_index = trading_date_index.tz_localize(None)

    cash_return_daily = rf_ytm / 100 / 365
    calendar_index = pd.date_range(cash_return_daily.index.min(), cash_return_daily.index.max(), freq='D')
    cash_return_daily = cash_return_daily.reindex(calendar_index, method='ffill')
    cash_return_PFL_value = (cash_return_daily + 1).cumprod()
    cash_return_PFL_value_reindex = cash_return_PFL_value.reindex(trading_date_index, method='ffill')
    return cash_return_PFL_value_reindex.pct_change()


def resample_last_date(data_daily, freq='M'):
    monthly_periods = data_daily.index.to_period(freq)
    data_monthly = data_daily.groupby(monthly_periods, group_keys=False).apply(lambda x: x.iloc[[-1]])
    return data_monthly

def compute_daily_weights_rets_from_rebal_targets(
    target_weights_at_rebal_time: pd.DataFrame,
    price_return_daily: pd.DataFrame,
    total_return_daily: pd.DataFrame,
    transaction_cost_rate: float,
    *,
    close_price: pd.DataFrame = None,
    entry_price: pd.DataFrame = None,
    entry_lag: int = 0,
):
    """리밸런싱 백테스트. entry_lag 거래일 후 entry_price로 진입.

    기본 동작 (close_price=None): 당일 종가 진입, close-to-close drift.
    close_price 제공 시: entry_price/entry_lag로 진입가격/지연 지정 가능.
      - entry_lag=0, entry_price=None → 당일 종가 진입
      - entry_lag=1, entry_price=open → T+1 시가 진입 (entry/close 분할)

    Returns: (after_cost, before_cost, daily_eod_weights, turnover_series)
    """
    _has_entry_price = entry_price is not None
    first_rebal = target_weights_at_rebal_time.index[0]
    if close_price is not None and not _has_entry_price:
        entry_price = close_price
    elif close_price is None:
        entry_price = None
    price_return_daily = price_return_daily[first_rebal:]
    total_return_daily = total_return_daily[first_rebal:]
    if close_price is not None:
        close_price = close_price[first_rebal:]
        entry_price = entry_price[first_rebal:]
    all_days = price_return_daily.index
    signal_dates = target_weights_at_rebal_time.index

    # --- Step A: 실행일 = 신호일 + entry_lag 거래일 ---
    locs = all_days.searchsorted(signal_dates)
    valid = locs + entry_lag < len(all_days)
    execution_dates = all_days[locs[valid] + entry_lag]

    target_weights_exec = target_weights_at_rebal_time.loc[signal_dates[valid]].copy()
    target_weights_exec.index = execution_dates

    # --- Step B: Weight drift ---
    drift_return = price_return_daily.copy()
    if close_price is not None:
        entry_to_close_return = close_price / entry_price - 1
        drift_return.loc[execution_dates] = entry_to_close_return.loc[execution_dates]

    anchor_series = pd.Series(execution_dates, index=execution_dates).reindex(all_days, method='ffill')
    anchor_series = anchor_series.dropna()
    drift_return = drift_return.loc[anchor_series.index]
    all_days = anchor_series.index

    ret_plus_one = drift_return + 1.0
    cumprod_to_date = ret_plus_one.groupby(anchor_series).cumprod()
    cumprod_groupby = cumprod_to_date.groupby(anchor_series)

    tw_daily = target_weights_exec.reindex(anchor_series.tolist()).set_index(all_days)
    unnormalized_values = tw_daily * cumprod_to_date
    daily_eod_weights = unnormalized_values.div(unnormalized_values.sum(axis=1), axis=0).fillna(0)

    # --- Step C: Turnover ---
    intra_period_return = cumprod_groupby.last()
    end_of_period_unnorm = target_weights_exec * intra_period_return
    end_of_period_weights = end_of_period_unnorm.div(end_of_period_unnorm.sum(axis=1), axis=0)
    turnover_by_stock = (target_weights_exec.fillna(0) - end_of_period_weights.shift(1).fillna(0)).abs()
    portfolio_turnover_series = turnover_by_stock.sum(axis=1)

    # --- Step D: 포트폴리오 수익률 ---
    sod_weights = daily_eod_weights.shift(1)
    if _has_entry_price:
        sod_weights.iloc[0] = 0
    else:
        sod_weights.iloc[0] = target_weights_exec.iloc[0]
    normal_return = (sod_weights * total_return_daily.loc[sod_weights.index]).sum(axis=1)

    pfl_return_before_cost = normal_return.copy()
    if _has_entry_price and close_price is not None:
        prev_close = close_price.shift(1)
        pre_entry_ret = (entry_price / prev_close - 1)
        post_entry_ret = (close_price / entry_price - 1)
        exec_return = (sod_weights.loc[execution_dates] * pre_entry_ret.loc[execution_dates]).sum(axis=1) + \
                      (target_weights_exec * post_entry_ret.loc[execution_dates]).sum(axis=1)
        pfl_return_before_cost.loc[execution_dates] = exec_return
    pfl_return_before_cost = pfl_return_before_cost.dropna()

    # --- Step E: 거래비용 ---
    transaction_cost = portfolio_turnover_series * -transaction_cost_rate
    pfl_return_after_cost = pfl_return_before_cost + transaction_cost.reindex(pfl_return_before_cost.index, fill_value=0)

    zero_dates = daily_eod_weights.index[daily_eod_weights.sum(axis=1) == 0]
    pfl_return_before_cost.loc[zero_dates] = np.nan
    pfl_return_after_cost.loc[zero_dates] = np.nan

    return pfl_return_after_cost, pfl_return_before_cost, daily_eod_weights, portfolio_turnover_series




def quantile(dataframe, q, axis=1):
    arr = dataframe.values.astype('float64')           # (n_dates, n_themes)
    np_axis = 1 if axis == 1 else 0
    boundaries = np.nanquantile(arr, [k / q for k in range(1, q)], axis=np_axis)
    # boundaries shape: (q-1, n_dates) for axis=1, (q-1, n_themes) for axis=0

    if np_axis == 1:
        cmp = arr[:, :, None] >= boundaries.T[:, None, :]  # (n_dates, n_themes, q-1)
    else:
        cmp = arr[:, :, None] >= boundaries.T[None, :, :]  # (n_dates, n_themes, q-1)

    labels = cmp.sum(axis=2).astype('float64') + 1    # (n_dates, n_themes), label 1~q
    labels[np.isnan(arr)] = np.nan
    return pd.DataFrame(labels, index=dataframe.index, columns=dataframe.columns)


def quantile_return_by_group(quantile_df: pd.DataFrame, return_df: pd.DataFrame) -> pd.DataFrame:
    """분위수별 평균 수익률 산출.

    Parameters
    ----------
    quantile_df : pd.DataFrame
        정수형 분위수 레이블 (1 ~ q). quantile() 함수의 출력값.
    return_df : pd.DataFrame
        quantile_df와 동일한 columns를 가지는 수익률 DataFrame.

    Returns
    -------
    pd.DataFrame
        index = 날짜, columns = 분위수 번호(int), values = 분위수 내 평균 수익률.
        NaN 행은 제거됨.
    """
    max_quantile = int(quantile_df.max().max())
    shifted_base = quantile_df.shift(1)
    q_series_dict = {}
    for q_num in range(1, max_quantile + 1):
        shifted = shifted_base.copy()
        shifted[shifted != q_num] = np.nan
        shifted[shifted == q_num] = return_df
        q_series_dict[q_num] = shifted.mean(axis=1)
    return pd.DataFrame(q_series_dict).dropna()


def cagr(return_df: pd.DataFrame) -> pd.Series:
    """구간 수익률 DataFrame/Series → 연환산 수익률(CAGR) 반환.

    Parameters
    ----------
    return_df : pd.DataFrame or pd.Series
        구간 수익률. index는 DatetimeIndex.

    Returns
    -------
    pd.Series
        columns(또는 name)을 index로 하는 CAGR 값.
    """
    cum_ret = (return_df + 1).prod()
    n_years = (return_df.index[-1] - return_df.index[0]).days / 365.25
    return cum_ret ** (1 / n_years) - 1


def annualized_turnover(turnover: pd.Series | pd.DataFrame, skip_first: bool = True):
    """턴오버를 연율화. Series→float, DataFrame→Series(컬럼별).

    Parameters
    ----------
    turnover : pd.Series | pd.DataFrame
        리밸런싱 시점별 턴오버. index=DatetimeIndex.
    skip_first : bool
        첫 번째 값(초기 진입 턴오버) 제외 여부 (default True).
    """
    ts = turnover.iloc[1:] if skip_first else turnover
    n_years = (ts.index[-1] - ts.index[0]).days / 365.25
    return ts.sum() / n_years


def rounding_target_weight(
    target_weight: pd.DataFrame,
    bm: pd.DataFrame,
    n_round: int = 3,
) -> pd.DataFrame:
    """포트폴리오 비중 반올림 후 합계 100% 보정.

    active weight(target - bm) 크기 순으로 round-robin 방식으로
    반올림 오차를 step 단위로 배분.
    - error > 0: 양수 active 종목에 +step 배분
    - error < 0: 음수 active 종목에 -step 차감
    공매도 제약(비중 < 0 방지) 적용 후 재정규화.

    Parameters
    ----------
    target_weight : pd.DataFrame
        리밸런싱 비중 (full precision). index=날짜, columns=종목.
    bm : pd.DataFrame
        벤치마크 비중. daily 기준, 함수 내부에서 target_weight 인덱스로 reindex.
    n_round : int
        소수점 자릿수 (default 3). step = 10^(-n_round).

    Returns
    -------
    pd.DataFrame
        반올림 + 오차 보정된 비중. sum(axis=1) ≈ 1.0.
    """
    step = 10 ** (-n_round)
    rounded = target_weight.round(n_round)
    error = 1.0 - rounded.sum(axis=1)
    n_steps = (error / step).round().astype(int)
    bm_aligned = bm.reindex(rounded.index, method='ffill')
    bm_aligned.columns = rounded.columns
    active = rounded - bm_aligned

    # error > 0: 양수 active 종목에 +step 배분
    pos_active = active.where(active > 0)
    pos_rank = pos_active.rank(axis=1, ascending=False, method='first')
    n_pos_cand = pos_active.notna().sum(axis=1).replace(0, 1)
    n_pos = n_steps.clip(lower=0)
    pos_base = n_pos // n_pos_cand
    pos_remainder = n_pos % n_pos_cand
    pos_adj = pos_rank.le(pos_remainder, axis=0).astype(int).add(pos_base, axis=0)
    pos_adj = pos_adj.where(pos_active.notna(), 0).fillna(0).mul(step)

    # error < 0: 음수 active 종목에 -step 차감
    neg_active = active.where(active < 0)
    neg_rank = neg_active.rank(axis=1, ascending=True, method='first')
    n_neg_cand = neg_active.notna().sum(axis=1).replace(0, 1)
    n_neg = (-n_steps).clip(lower=0)
    neg_base = n_neg // n_neg_cand
    neg_remainder = n_neg % n_neg_cand
    neg_adj = neg_rank.le(neg_remainder, axis=0).astype(int).add(neg_base, axis=0)
    neg_adj = neg_adj.where(neg_active.notna(), 0).fillna(0).mul(step)

    result = rounded + pos_adj - neg_adj
    result = result.clip(lower=0)
    row_sum = result.sum(axis=1)
    result = result.div(row_sum.where(row_sum != 0, 1), axis=0)
    return result