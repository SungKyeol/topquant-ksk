import numpy as np
import pandas as pd
import pytest

from topquant_ksk.tools import (
    annualized_turnover,
    cagr,
    quantile,
    quantile_return_by_group,
    resample_last_date,
    rounding_target_weight,
)

BUS_DAYS_1Y = pd.date_range("2020-01-01", periods=252, freq="B")
BUS_DAYS_2Y = pd.date_range("2020-01-01", periods=504, freq="B")


class TestCagr:
    def test_zero_returns_give_zero_cagr(self):
        ret = pd.Series(0.0, index=BUS_DAYS_1Y)
        assert abs(cagr(ret)) < 1e-10

    def test_positive_returns_give_positive_cagr(self):
        ret = pd.Series(0.001, index=BUS_DAYS_1Y)
        assert cagr(ret) > 0

    def test_dataframe_returns_series(self):
        df = pd.DataFrame(
            {"a": [0.001] * len(BUS_DAYS_1Y), "b": [0.002] * len(BUS_DAYS_1Y)},
            index=BUS_DAYS_1Y,
        )
        result = cagr(df)
        assert isinstance(result, pd.Series)
        assert list(result.index) == ["a", "b"]
        assert result["b"] > result["a"]

    def test_known_cagr(self):
        # Single year (365 days) with constant daily return r:
        # CAGR ≈ (1+r)^252 - 1 for business-day series
        idx = pd.date_range("2020-01-01", "2020-12-31", freq="B")
        daily_r = 0.001
        ret = pd.Series(daily_r, index=idx)
        result = cagr(ret)
        n_days = len(idx)
        expected = (1 + daily_r) ** n_days - 1
        # cagr raises to 1/n_years where n_years = (idx[-1]-idx[0]).days/365.25
        n_years = (idx[-1] - idx[0]).days / 365.25
        expected_cagr = (1 + daily_r) ** (n_days / n_years) - 1
        assert abs(result - expected_cagr) < 0.01


class TestAnnualizedTurnover:
    def test_monthly_turnover_annualized(self):
        # 12 monthly rebalances of 0.1 over 1 year → ~1.2 annual turnover
        idx = pd.date_range("2020-01-31", periods=13, freq="M")
        to = pd.Series(0.1, index=idx)
        result = annualized_turnover(to, skip_first=True)
        assert abs(result - 1.2) < 0.15

    def test_skip_first_drops_initial_large_turnover(self):
        # First period has large turnover (initial portfolio construction); rest are normal
        idx = pd.date_range("2020-01-31", periods=13, freq="M")
        values = [1.0] + [0.05] * 12  # first rebalance is 100% turnover
        to = pd.Series(values, index=idx)
        with_skip = annualized_turnover(to, skip_first=True)
        without_skip = annualized_turnover(to, skip_first=False)
        assert without_skip > with_skip

    def test_dataframe_input_returns_series(self):
        idx = pd.date_range("2020-01-31", periods=13, freq="M")
        df = pd.DataFrame({"a": 0.1, "b": 0.2}, index=idx)
        result = annualized_turnover(df, skip_first=True)
        assert isinstance(result, pd.Series)
        assert result["b"] > result["a"]


class TestQuantile:
    def test_basic_three_bins_axis1(self):
        idx = pd.date_range("2020-01-01", periods=1, freq="B")
        df = pd.DataFrame({"a": [1.0], "b": [2.0], "c": [3.0]}, index=idx)
        result = quantile(df, q=3, axis=1)
        # lowest → 1, middle → 2, highest → 3
        assert result.at[idx[0], "a"] == 1.0
        assert result.at[idx[0], "b"] == 2.0
        assert result.at[idx[0], "c"] == 3.0

    def test_nan_preserved(self):
        idx = pd.date_range("2020-01-01", periods=1, freq="B")
        df = pd.DataFrame({"a": [np.nan], "b": [2.0], "c": [3.0]}, index=idx)
        result = quantile(df, q=2, axis=1)
        assert np.isnan(result.at[idx[0], "a"])

    def test_label_range_1_to_q(self):
        idx = pd.date_range("2020-01-01", periods=5, freq="B")
        data = np.tile([1.0, 2.0, 3.0, 4.0, 5.0], (5, 1))
        df = pd.DataFrame(data, index=idx)
        result = quantile(df, q=5, axis=1)
        assert result.values.min() >= 1
        assert result.values.max() <= 5

    def test_output_shape_matches_input(self):
        idx = pd.date_range("2020-01-01", periods=10, freq="B")
        df = pd.DataFrame(np.random.rand(10, 6), index=idx)
        result = quantile(df, q=3, axis=1)
        assert result.shape == df.shape


class TestQuantileReturnByGroup:
    def test_returns_dataframe(self):
        idx = pd.date_range("2020-01-01", periods=20, freq="B")
        # Each row: stock a→Q1, b→Q2, c→Q3
        q_df = pd.DataFrame(
            {"a": [1.0] * 20, "b": [2.0] * 20, "c": [3.0] * 20},
            index=idx,
        )
        ret_df = pd.DataFrame(
            np.random.rand(20, 3) * 0.01,
            index=idx,
            columns=["a", "b", "c"],
        )
        result = quantile_return_by_group(q_df, ret_df)
        assert isinstance(result, pd.DataFrame)
        # columns are quantile numbers 1, 2, 3
        assert set(result.columns) == {1, 2, 3}


class TestResampleLastDate:
    def test_monthly_resampling_row_count(self):
        idx = pd.date_range("2020-01-01", periods=60, freq="B")
        data = pd.Series(range(60), index=idx, dtype=float)
        result = resample_last_date(data, freq="M")
        # ~3 months of business days → 2-4 rows
        assert 2 <= len(result) <= 4

    def test_each_entry_is_period_last(self):
        # Values are sequential; last per month must be greater than previous month's last
        idx = pd.date_range("2020-01-01", periods=42, freq="B")
        data = pd.Series(range(42), index=idx, dtype=float)
        result = resample_last_date(data, freq="M")
        for i in range(len(result) - 1):
            assert result.iloc[i + 1] > result.iloc[i]


class TestRoundingTargetWeight:
    def test_weights_sum_to_one(self):
        idx = pd.date_range("2020-01-31", periods=3, freq="M")
        target = pd.DataFrame(
            {
                "a": [0.33334, 0.33334, 0.33334],
                "b": [0.33333, 0.33333, 0.33333],
                "c": [0.33333, 0.33333, 0.33333],
            },
            index=idx,
        )
        bm = pd.DataFrame(
            {
                "a": [0.333, 0.333, 0.333],
                "b": [0.333, 0.333, 0.333],
                "c": [0.334, 0.334, 0.334],
            },
            index=idx,
        )
        result = rounding_target_weight(target, bm, n_round=3)
        row_sums = result.sum(axis=1)
        np.testing.assert_allclose(row_sums.values, 1.0, atol=1e-9)

    def test_no_short_positions(self):
        idx = pd.date_range("2020-01-31", periods=2, freq="M")
        target = pd.DataFrame({"a": [0.5, 0.5], "b": [0.5, 0.5]}, index=idx)
        bm = pd.DataFrame({"a": [0.5, 0.5], "b": [0.5, 0.5]}, index=idx)
        result = rounding_target_weight(target, bm, n_round=2)
        assert (result.values >= 0).all()

    def test_output_shape_matches_input(self):
        idx = pd.date_range("2020-01-31", periods=4, freq="M")
        n_stocks = 5
        rng = np.random.default_rng(0)
        raw = rng.dirichlet(np.ones(n_stocks), size=4)
        target = pd.DataFrame(raw, index=idx)
        bm = pd.DataFrame(raw * 0.9, index=idx)
        result = rounding_target_weight(target, bm, n_round=3)
        assert result.shape == target.shape


# --- reconstruct_stale_tr_with_pr ---

from topquant_ksk.tools import reconstruct_stale_tr_with_pr


def _make_index_df(data_by_ticker):
    """
    data_by_ticker: {
        (ticker, index_name): {'FG_PRICE': Series, 'FG_TOTAL_RET_IDX': Series, ...}
    }
    → 3-level MultiIndex column DataFrame
    """
    frames = []
    for (ticker, idx_name), items in data_by_ticker.items():
        sub = pd.DataFrame(items)
        sub.columns = pd.MultiIndex.from_product([[ticker], [idx_name], sub.columns],
                                                 names=['ticker', 'index_name', 'item_name'])
        frames.append(sub)
    df = pd.concat(frames, axis=1)
    df.columns.names = ['ticker', 'index_name', 'item_name']
    return df


class TestReconstructStaleTrWithPr:
    def test_happy_path_single_stale_ticker(self):
        # 5영업일: 첫 3일 TR stale, 마지막 2일 clean
        idx = pd.date_range('2000-01-03', periods=5, freq='B')
        pr = pd.Series([100.0, 101.0, 102.0, 103.0, 104.0], index=idx)
        tr = pd.Series([500.0, 500.0, 500.0, 520.0, 530.0], index=idx)  # stale: day0-2, clean: day3-4
        df = _make_index_df({('T1', 'Test Index'): {'FG_PRICE': pr, 'FG_TOTAL_RET_IDX': tr}})

        result = reconstruct_stale_tr_with_pr(df, verbose=False)

        new_tr = result[('T1', 'Test Index', 'FG_TOTAL_RET_IDX')]
        # last stale day = day2 (index 2), anchor = day3 (index 3)
        # scale = 520 / 103
        scale = 520.0 / 103.0
        # day0-2 should be pr * scale; day3-4 unchanged
        expected_day0 = 100.0 * scale
        expected_day1 = 101.0 * scale
        expected_day2 = 102.0 * scale
        assert abs(new_tr.iloc[0] - expected_day0) < 1e-9
        assert abs(new_tr.iloc[1] - expected_day1) < 1e-9
        assert abs(new_tr.iloc[2] - expected_day2) < 1e-9
        assert new_tr.iloc[3] == 520.0  # anchor preserved
        assert new_tr.iloc[4] == 530.0  # post-anchor preserved

    def test_original_df_not_mutated(self):
        idx = pd.date_range('2000-01-03', periods=5, freq='B')
        pr = pd.Series([100.0, 101.0, 102.0, 103.0, 104.0], index=idx)
        tr = pd.Series([500.0, 500.0, 500.0, 520.0, 530.0], index=idx)
        df = _make_index_df({('T1', 'Test Index'): {'FG_PRICE': pr, 'FG_TOTAL_RET_IDX': tr}})
        df_copy = df.copy(deep=True)

        _ = reconstruct_stale_tr_with_pr(df, verbose=False)

        pd.testing.assert_frame_equal(df, df_copy)
