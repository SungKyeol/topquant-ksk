import numpy as np
import pandas as pd
import pytest

from topquant_ksk.risk_return_metrics import (
    YearlyMonthlyERDataFrame,
    get_RiskReturnProfile,
    get_yearly_monthly_ER,
)

np.random.seed(42)
PERIODS = 504  # ~2 trading years
IDX = pd.date_range("2020-01-01", periods=PERIODS, freq="B")
STRAT_RET = pd.Series(np.random.normal(0.001, 0.01, PERIODS), index=IDX)
BM_RET = pd.Series(np.random.normal(0.0008, 0.01, PERIODS), index=IDX)
CASH_RET = pd.Series(0.00005, index=IDX)  # ~1.3% annual


class TestGetRiskReturnProfile:
    def test_returns_dataframe(self):
        result = get_RiskReturnProfile(STRAT_RET, CASH_RET)
        assert isinstance(result, pd.DataFrame)

    def test_core_columns_present(self):
        result = get_RiskReturnProfile(STRAT_RET, CASH_RET)
        for col in [
            "CAGR(%)",
            "STD_annualized(%)",
            "Sharpe_Ratio",
            "MDD(%)",
            "UnderWaterPeriod(년)",
            "Weekly Hit Ratio(%)",
        ]:
            assert col in result.columns, f"Missing column: {col}"

    def test_mdd_is_nonpositive(self):
        result = get_RiskReturnProfile(STRAT_RET, CASH_RET)
        assert float(result["MDD(%)"].iloc[0]) <= 0

    def test_std_is_positive(self):
        result = get_RiskReturnProfile(STRAT_RET, CASH_RET)
        assert float(result["STD_annualized(%)"].iloc[0]) > 0

    def test_with_benchmark_adds_benchmark_row(self):
        result = get_RiskReturnProfile(STRAT_RET, CASH_RET, BM_ret=BM_RET)
        assert len(result) == 2
        assert result.index[-1] == "Benchmark"

    def test_with_benchmark_adds_relative_columns(self):
        result = get_RiskReturnProfile(STRAT_RET, CASH_RET, BM_ret=BM_RET)
        for col in ["Information_Ratio", "tracking_error(%)", "BM_ret excess_return(%)"]:
            assert col in result.columns, f"Missing column: {col}"

    def test_with_turnover_adds_column(self):
        turnover = pd.Series(0.05, index=IDX)
        result = get_RiskReturnProfile(STRAT_RET, CASH_RET, turnover=turnover)
        assert "Annualized Turnover(%)" in result.columns

    def test_dataframe_input_one_row_per_strategy(self):
        df = pd.DataFrame({"s1": STRAT_RET, "s2": STRAT_RET * 1.1})
        result = get_RiskReturnProfile(df, CASH_RET)
        assert len(result) == 2

    def test_series_input_auto_converts(self):
        # Series input should work identically to single-column DataFrame
        result_series = get_RiskReturnProfile(STRAT_RET, CASH_RET)
        result_df = get_RiskReturnProfile(STRAT_RET.to_frame("strategy"), CASH_RET)
        assert len(result_series) == len(result_df)

    def test_weekly_hit_ratio_between_0_and_100(self):
        result = get_RiskReturnProfile(STRAT_RET, CASH_RET)
        ratio = float(result["Weekly Hit Ratio(%)"].iloc[0])
        assert 0 <= ratio <= 100


class TestGetYearlyMonthlyEr:
    def test_returns_yearly_monthly_er_dataframe(self):
        result = get_yearly_monthly_ER(STRAT_RET, BM_RET)
        assert isinstance(result, YearlyMonthlyERDataFrame)

    def test_has_required_base_columns(self):
        result = get_yearly_monthly_ER(STRAT_RET, BM_RET)
        for col in ["Strategy", "BM", "ER"]:
            assert col in result.columns

    def test_last_row_is_gmean(self):
        result = get_yearly_monthly_ER(STRAT_RET, BM_RET)
        assert result.index[-1] == "gmean"

    def test_has_monthly_columns(self):
        result = get_yearly_monthly_ER(STRAT_RET, BM_RET)
        month_cols = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"}
        present = set(result.columns) & month_cols
        assert len(present) > 0

    def test_er_equals_strategy_minus_bm(self):
        result = get_yearly_monthly_ER(STRAT_RET, BM_RET)
        # Exclude gmean row
        year_rows = result.iloc[:-1]
        er_diff = (year_rows["Strategy"] - year_rows["BM"]).round(1)
        np.testing.assert_allclose(er_diff.values, year_rows["ER"].values, atol=0.2)

    def test_output_scaled_to_percent(self):
        # Values should be in percent (i.e., ~order of magnitude 1-50, not 0.001)
        result = get_yearly_monthly_ER(STRAT_RET, BM_RET)
        # Strategy annual return should be in % range, not fraction range
        strategy_vals = result["Strategy"].dropna()
        assert strategy_vals.abs().max() > 0.1  # definitely not fractions


class TestYearlyMonthlyErDataFrame:
    def test_is_pandas_dataframe_subclass(self):
        result = get_yearly_monthly_ER(STRAT_RET, BM_RET)
        assert isinstance(result, pd.DataFrame)

    def test_constructor_preserves_type(self):
        result = get_yearly_monthly_ER(STRAT_RET, BM_RET)
        # Slicing should preserve the custom type via _constructor
        subset = result.iloc[:2]
        assert isinstance(subset, YearlyMonthlyERDataFrame)
