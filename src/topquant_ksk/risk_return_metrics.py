import pandas as pd
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

class YearlyMonthlyERDataFrame(pd.DataFrame):
    """get_yearly_monthly_ER 결과를 담는 DataFrame with heatmap method"""

    @property
    def _constructor(self):
        return YearlyMonthlyERDataFrame

    def heatmap(self, figsize=(18, 10), fontsize=9):
        """ER 컬럼과 월별 ER에만 RdYlBu_r colormap 조건부 서식 적용 (matplotlib)"""

        fig, ax = plt.subplots(figsize=figsize)

        # 컬럼 분류
        er_cols = ['ER']
        monthly_cols = [c for c in self.columns if str(c).endswith('월')]
        er_col_idx = [self.columns.get_loc(c) for c in er_cols]
        monthly_col_idx = [self.columns.get_loc(c) for c in monthly_cols]

        # ER 범위와 Monthly ER 범위 각각 계산 (0 기준 대칭)
        er_max_abs = self[er_cols].abs().max().max()
        monthly_max_abs = self[monthly_cols].abs().max().max()

        norm_er = mcolors.TwoSlopeNorm(vmin=-er_max_abs, vcenter=0, vmax=er_max_abs)
        norm_monthly = mcolors.TwoSlopeNorm(vmin=-monthly_max_abs, vcenter=0, vmax=monthly_max_abs)
        cmap = plt.cm.RdYlBu_r

        # 테이블 그리기
        ax.axis('off')
        table = ax.table(
            cellText=self.values,
            colLabels=self.columns.tolist(),
            rowLabels=self.index.tolist(),
            loc='center',
            cellLoc='center'
        )
        table.auto_set_font_size(False)
        table.set_fontsize(fontsize)
        table.scale(1.2, 1.5)

        last_row = len(self) - 1

        # ER 컬럼 색상 적용 (마지막 행 제외)
        for i in range(last_row):
            for j in er_col_idx:
                val = self.iloc[i, j]
                if pd.notna(val):
                    table[(i + 1, j)].set_facecolor(cmap(norm_er(val)))

        # 월별 ER 컬럼 색상 적용 (마지막 행 제외)
        for i in range(last_row):
            for j in monthly_col_idx:
                val = self.iloc[i, j]
                if pd.notna(val):
                    table[(i + 1, j)].set_facecolor(cmap(norm_monthly(val)))

        # 마지막 행 월별 컬럼: 별도 색상 범위 적용
        gmean_monthly = self.iloc[-1][monthly_cols]
        gmean_max_abs = gmean_monthly.abs().max()
        norm_gmean = mcolors.TwoSlopeNorm(vmin=-gmean_max_abs, vcenter=0, vmax=gmean_max_abs)
        for j in monthly_col_idx:
            val = self.iloc[last_row, j]
            if pd.notna(val):
                table[(last_row + 1, j)].set_facecolor(cmap(norm_gmean(val)))

        plt.tight_layout()
        plt.show()


def get_RiskReturnProfile(rebalencing_ret: pd.DataFrame, cash_return_daily_BenchmarkFrequency: pd.Series, BM_ret: pd.Series | None = None):
    """
    수익률 데이터를 받아 주요 성과 지표를 계산합니다.
    모든 UnderwaterPeriod 계산에 .apply 없는 완전 벡터화 코드를 사용합니다.
    """
    
    def _vectorized_max_underwater_period(value_df: pd.DataFrame) -> pd.Series:
        """가치 DataFrame을 받아 최대 손실 기간(연 단위) Series를 계산하는 내부 헬퍼 함수"""
        is_underwater = value_df < value_df.cummax()
        cum_underwater = is_underwater.cumsum()
        reset_points = cum_underwater.where(~is_underwater).ffill().fillna(0)
        consecutive_days = cum_underwater - reset_points
        max_days = consecutive_days.max()
        return (max_days / 252).round(1)

    # --- 1. 전략(들)에 대한 공통 성과 지표 계산 ---
    CAGR = (np.exp(np.log(rebalencing_ret + 1).mean() * 252) - 1).round(3) * 100
    STD_annualized = (rebalencing_ret.std() * np.sqrt(252)).round(3) * 100
    
    excess_ret = rebalencing_ret.subtract(cash_return_daily_BenchmarkFrequency.reindex(rebalencing_ret.index, method='ffill'), axis=0)
    excess_ret_yearly = (np.exp(np.log(excess_ret + 1).mean() * 252) - 1)
    Sharpe_Ratio = (excess_ret_yearly / (rebalencing_ret.std() * np.sqrt(252))).round(3)

    # ★★★ 주간 승률(Weekly Hit Ratio) 계산 추가 ★★★
    weekly_returns = (rebalencing_ret + 1).resample('W-FRI').prod() - 1
    Weekly_Hit_Ratio = ((weekly_returns > 0).sum() / weekly_returns.count()).round(3) * 100
    
    pfl_value = (rebalencing_ret + 1).cumprod()
    MDD = (pfl_value / pfl_value.cummax() - 1).min().round(3) * 100
    MDD_date = (pfl_value / pfl_value.cummax() - 1).idxmin().astype(str).str[:7]
    
    # 벡터화된 함수를 사용하여 절대 최대 손실 기간 계산
    UnderWaterPeriod = _vectorized_max_underwater_period(pfl_value)

    # 기간별 수익률
    ret_1M = ((rebalencing_ret.iloc[-21:] + 1).prod() - 1).round(3) * 100
    ret_3M = ((rebalencing_ret.iloc[-21*3:] + 1).prod() - 1).round(3) * 100
    ret_6M = ((rebalencing_ret.iloc[-21*6:] + 1).prod() - 1).round(3) * 100
    ret_1Y = ((rebalencing_ret.iloc[-252:] + 1).prod() - 1).round(2) * 100
    ret_3Y = ((rebalencing_ret.iloc[-252*3:] + 1).prod() - 1).round(2) * 100
    
    metrics_list = [
        CAGR, STD_annualized, Weekly_Hit_Ratio, Sharpe_Ratio, MDD, MDD_date, UnderWaterPeriod,
        ret_1M, ret_3M, ret_6M, ret_1Y, ret_3Y
    ]
    index_list = [
        'CAGR(%)', 'STD_annualized(%)', 'Weekly Hit Ratio(%)', 'Sharpe_Ratio', 'MDD(%)', 'MDD시점', 'UnderWaterPeriod(년)',
        '1M Ret(%)', '3M Ret(%)', '6M Ret(%)', '1Y Ret(%)', '3Y Ret(%)'
    ]
    
    matric = pd.DataFrame(metrics_list, index=index_list).T
    
    if BM_ret is not None:
        aligned_ret, aligned_bm = rebalencing_ret.align(BM_ret, join='inner', axis=0)
        
        # 가. BM_ret 자체의 공통 성과 지표 계산
        BM_CAGR = round(np.exp(np.log(aligned_bm + 1).mean() * 252) - 1, 3) * 100
        BM_STD = round(aligned_bm.std() * np.sqrt(252), 3) * 100
        bm_excess_ret = aligned_bm.subtract(cash_return_daily_BenchmarkFrequency.reindex(aligned_bm.index, method='ffill'))
        bm_excess_ret_yearly = np.exp(np.log(bm_excess_ret + 1).mean() * 252) - 1
        BM_Sharpe = round(bm_excess_ret_yearly / (aligned_bm.std() * np.sqrt(252)), 3)
        bm_weekly_returns = (aligned_bm + 1).resample('W-FRI').prod() - 1
        BM_Weekly_Hit_Ratio = round((bm_weekly_returns > 0).mean(), 3) * 100
        bm_value = (aligned_bm + 1).cumprod()
        BM_MDD = round((bm_value / bm_value.cummax() - 1).min(), 3) * 100
        BM_MDD_date = (bm_value / bm_value.cummax() - 1).idxmin().strftime('%Y-%m')
        # BM_ret(Series)를 DataFrame으로 변환하여 헬퍼 함수 사용 후 값 추출
        BM_UnderWaterPeriod = _vectorized_max_underwater_period(bm_value.to_frame()).iloc[0]
        BM_ret_1M = round((aligned_bm.iloc[-21:] + 1).prod() - 1, 3) * 100
        BM_ret_3M = round((aligned_bm.iloc[-21*3:] + 1).prod() - 1, 3) * 100
        BM_ret_6M = round((aligned_bm.iloc[-21*6:] + 1).prod() - 1, 3) * 100
        BM_ret_1Y = round((aligned_bm.iloc[-252:] + 1).prod() - 1, 2) * 100
        BM_ret_3Y = round((aligned_bm.iloc[-252*3:] + 1).prod() - 1, 2) * 100
        
        # 나. 전략의 BM_ret 대비 상대 성과 지표 계산
        excess_return_vs_bm = aligned_ret.subtract(aligned_bm, axis=0)
        annualized_excess_return = (np.exp(np.log(excess_return_vs_bm + 1).mean() * 252) - 1)
        tracking_error = excess_return_vs_bm.std() * np.sqrt(252)
        information_ratio = (annualized_excess_return / tracking_error).round(3)
        relative_value = (excess_return_vs_bm + 1).cumprod()
        relative_drawdown = (relative_value / relative_value.cummax() - 1)
        max_relative_drawdown = relative_drawdown.min().round(3) * 100
        max_relative_drawdown_date = relative_drawdown.idxmin().astype(str).str[:7]
        # 벡터화된 함수를 사용하여 상대 최대 손실 기간 계산
        max_relative_underwater_duration = _vectorized_max_underwater_period(relative_value)

        # ★★★ 오류 수정: .gt() 메서드와 axis=0 옵션 사용 ★★★
        aligned_weekly_ret, aligned_bm_weekly_ret = weekly_returns.align(bm_weekly_returns, join='inner', axis=0)
        relative_weekly_hit_ratio = (aligned_weekly_ret.gt(aligned_bm_weekly_ret, axis=0).mean()).round(3) * 100
   
        # 다. 최종 결과 테이블에 상대 성과 지표 컬럼 추가
        matric['BM_ret excess_return(%)'] = round(annualized_excess_return * 100, 1)
        matric['tracking_error(%)'] = round(tracking_error * 100, 1)
        matric['Information_Ratio'] = information_ratio
        matric['BM대비주간승률(%)'] = relative_weekly_hit_ratio
        matric['BM대비최대손실(%)'] = max_relative_drawdown
        matric['BM대비최대손실시점'] = max_relative_drawdown_date
        matric['BM_ret Max Underwater(년)'] = max_relative_underwater_duration

        # 라. BM_ret 성과 행 생성 및 추가
        bm_metrics_row = pd.Series(name='Benchmark', dtype=object)
        bm_metrics_row['CAGR(%)'] = BM_CAGR
        bm_metrics_row['STD_annualized(%)'] = BM_STD
        bm_metrics_row['Weekly Hit Ratio(%)'] = BM_Weekly_Hit_Ratio
        bm_metrics_row['Sharpe_Ratio'] = BM_Sharpe
        bm_metrics_row['MDD(%)'] = BM_MDD
        bm_metrics_row['MDD시점'] = BM_MDD_date
        bm_metrics_row['UnderWaterPeriod(년)'] = BM_UnderWaterPeriod
        bm_metrics_row['1M Ret(%)'] = BM_ret_1M
        bm_metrics_row['3M Ret(%)'] = BM_ret_3M
        bm_metrics_row['6M Ret(%)'] = BM_ret_6M
        bm_metrics_row['1Y Ret(%)'] = BM_ret_1Y
        bm_metrics_row['3Y Ret(%)'] = BM_ret_3Y
        bm_metrics_row['BM_ret excess_return(%)'] = '-'
        bm_metrics_row['tracking_error(%)'] = '-'
        bm_metrics_row['Information_Ratio'] = '-'
        bm_metrics_row['BM대비주간승률(%)'] = '-'
        bm_metrics_row['BM대비최대손실(%)'] = '-'
        bm_metrics_row['BM대비최대손실시점'] = '-'
        bm_metrics_row['BM_ret Max Underwater(년)'] = '-'

        matric = pd.concat([matric, bm_metrics_row.to_frame().T])

    return matric

def get_yearly_monthly_ER(strategy_return: pd.Series, BM_return: pd.Series) -> YearlyMonthlyERDataFrame:
    """
    전략 수익률과 벤치마크 수익률을 받아 연간/월별 초과수익률 계산

    Parameters:
        strategy_return: 일별 전략 수익률 Series
        BM_return: 일별 벤치마크 수익률 Series

    Returns:
        YearlyMonthlyERDataFrame with columns: Strategy, BM, ER, 1~12 (월별 ER)
        - .heatmap() 메서드로 시각화 가능
    """
    # 공통 인덱스로 정렬 (NA 제거)
    common_idx = strategy_return.dropna().index.intersection(BM_return.dropna().index)
    strategy_return = strategy_return.loc[common_idx]
    BM_return = BM_return.loc[common_idx]

    # 일별 수익률 합치기
    daily_ret = pd.concat([strategy_return, BM_return], axis=1)
    daily_ret.columns = ['Strategy', 'BM']

    # 연간 수익률 계산
    yearly_return = daily_ret.groupby(daily_ret.index.year).apply(lambda x: (x+1).prod()-1)
    yearly_return['ER'] = yearly_return['Strategy'] - yearly_return['BM']

    # 월별 수익률 계산
    monthly_return = daily_ret.groupby(
        [daily_ret.index.year, daily_ret.index.month]
    ).apply(lambda x: (x+1).prod()-1)
    monthly_return.index.names = ['year', 'month']

    # 월별 ER 계산 및 피벗
    monthly_ER = (monthly_return['Strategy'] - monthly_return['BM']).unstack(level='month')
    monthly_ER.columns = [f'{m}월' for m in monthly_ER.columns]

    # 연간 수익률에 월별 ER 병합
    result = yearly_return.join(monthly_ER)

    # 기하평균 행 추가 (벡터 연산, NaN 제외)
    log_returns = np.log(result + 1)
    gmean_row = np.exp(log_returns.mean()) - 1
    gmean_row.name = 'gmean'
    result = pd.concat([result, gmean_row.to_frame().T])

    return YearlyMonthlyERDataFrame((result * 100).round(2))
