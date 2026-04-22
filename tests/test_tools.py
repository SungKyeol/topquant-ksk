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


def _make_frozen_then_clean(n_frozen=80, n_clean=20, start_tr=500.0, start_pr=100.0, seed=0):
    """Create (pr, tr) series: first n_frozen days TR frozen (constant), then n_clean days moving."""
    n = n_frozen + n_clean
    idx = pd.date_range('2000-01-03', periods=n, freq='B')
    rng = np.random.default_rng(seed)
    pr_vals = start_pr * np.cumprod(1 + rng.normal(0.0005, 0.01, size=n))
    tr_vals = np.full(n, start_tr)
    for i in range(n_frozen, n):
        tr_vals[i] = tr_vals[i - 1] * (1 + rng.normal(0.0005, 0.01))
    return pd.Series(pr_vals, index=idx), pd.Series(tr_vals, index=idx)


class TestReconstructStaleTrWithPr:
    def test_happy_path_frozen_then_clean(self):
        """80일 frozen + 20일 clean → block 전체가 재구성."""
        pr, tr = _make_frozen_then_clean(n_frozen=80, n_clean=20)
        df = _make_index_df({('T1', 'Test'): {'FG_PRICE': pr, 'FG_TOTAL_RET_IDX': tr}})

        result = reconstruct_stale_tr_with_pr(df, verbose=False)

        new_tr = result[('T1', 'Test', 'FG_TOTAL_RET_IDX')]
        orig_tr = df[('T1', 'Test', 'FG_TOTAL_RET_IDX')]

        # Anchor = day 80 (첫 clean day), scale = TR[80]/PR[80]
        scale = orig_tr.iloc[80] / pr.iloc[80]
        expected_frozen = pr.iloc[:80] * scale

        # frozen 구간(0~79)이 PR*scale로 대체됨
        assert np.allclose(new_tr.iloc[:80].values, expected_frozen.values, atol=1e-9)
        # clean 구간(80+)은 원본 보존
        pd.testing.assert_series_equal(new_tr.iloc[80:], orig_tr.iloc[80:], check_names=False)

    def test_original_df_not_mutated(self):
        pr, tr = _make_frozen_then_clean()
        df = _make_index_df({('T1', 'Test'): {'FG_PRICE': pr, 'FG_TOTAL_RET_IDX': tr}})
        df_copy = df.copy(deep=True)

        _ = reconstruct_stale_tr_with_pr(df, verbose=False)

        pd.testing.assert_frame_equal(df, df_copy)

    def test_no_stale_returns_unchanged(self):
        """TR이 매일 움직이는 정상 시리즈 → 건드리지 않음."""
        n = 100
        idx = pd.date_range('2000-01-03', periods=n, freq='B')
        rng = np.random.default_rng(42)
        pr = pd.Series(100.0 * np.cumprod(1 + rng.normal(0.0005, 0.01, size=n)), index=idx)
        tr = pd.Series(500.0 * np.cumprod(1 + rng.normal(0.0005, 0.01, size=n)), index=idx)
        df = _make_index_df({('T1', 'Clean Index'): {'FG_PRICE': pr, 'FG_TOTAL_RET_IDX': tr}})

        result = reconstruct_stale_tr_with_pr(df, verbose=False)

        pd.testing.assert_frame_equal(result, df)

    def test_ticker_missing_tr_is_skipped(self):
        idx = pd.date_range('2000-01-03', periods=5, freq='B')
        pr = pd.Series([100.0, 101.0, 102.0, 103.0, 104.0], index=idx)
        df = _make_index_df({('T1', 'Only PR'): {'FG_PRICE': pr}})

        result = reconstruct_stale_tr_with_pr(df, verbose=False)

        pd.testing.assert_frame_equal(result, df)

    def test_ticker_missing_pr_is_skipped(self):
        idx = pd.date_range('2000-01-03', periods=5, freq='B')
        tr = pd.Series([500.0, 500.0, 500.0, 520.0, 530.0], index=idx)
        df = _make_index_df({('T1', 'Only TR'): {'FG_TOTAL_RET_IDX': tr}})

        result = reconstruct_stale_tr_with_pr(df, verbose=False)

        pd.testing.assert_frame_equal(result, df)

    def test_multi_ticker_mix(self):
        """T1 frozen → 재구성. T2 clean → 유지. T3 PR only → 유지."""
        pr_frozen, tr_frozen = _make_frozen_then_clean(n_frozen=80, n_clean=20, seed=0)

        # T2: clean 전체
        n = 100
        idx = pd.date_range('2000-01-03', periods=n, freq='B')
        rng = np.random.default_rng(10)
        pr_clean = pd.Series(200.0 * np.cumprod(1 + rng.normal(0.0005, 0.01, size=n)), index=idx)
        tr_clean = pd.Series(800.0 * np.cumprod(1 + rng.normal(0.0005, 0.01, size=n)), index=idx)

        # T3: PR only
        pr_only = pd.Series(50.0 + np.arange(n) * 0.1, index=idx)

        df = _make_index_df({
            ('T1', 'Frozen'): {'FG_PRICE': pr_frozen, 'FG_TOTAL_RET_IDX': tr_frozen},
            ('T2', 'Clean'): {'FG_PRICE': pr_clean, 'FG_TOTAL_RET_IDX': tr_clean},
            ('T3', 'PROnly'): {'FG_PRICE': pr_only},
        })

        result = reconstruct_stale_tr_with_pr(df, verbose=False)

        # T1 frozen 구간 재구성
        new_tr_t1 = result[('T1', 'Frozen', 'FG_TOTAL_RET_IDX')]
        scale = tr_frozen.iloc[80] / pr_frozen.iloc[80]
        assert np.allclose(new_tr_t1.iloc[:80].values, (pr_frozen.iloc[:80] * scale).values, atol=1e-9)
        # T2 clean 원본 유지
        pd.testing.assert_series_equal(
            result[('T2', 'Clean', 'FG_TOTAL_RET_IDX')],
            df[('T2', 'Clean', 'FG_TOTAL_RET_IDX')],
        )
        # T3 PR only 원본 유지
        pd.testing.assert_series_equal(
            result[('T3', 'PROnly', 'FG_PRICE')],
            df[('T3', 'PROnly', 'FG_PRICE')],
        )

    def test_all_stale_emits_warning_and_skips(self, capsys):
        """전체 구간 frozen → anchor 없음 → warning + skip."""
        n = 100
        idx = pd.date_range('2000-01-03', periods=n, freq='B')
        rng = np.random.default_rng(0)
        pr = pd.Series(100.0 * np.cumprod(1 + rng.normal(0.0005, 0.01, size=n)), index=idx)
        tr = pd.Series([500.0] * n, index=idx)  # 전부 frozen
        df = _make_index_df({('T1', 'AllFrozen'): {'FG_PRICE': pr, 'FG_TOTAL_RET_IDX': tr}})
        df_copy = df.copy(deep=True)

        result = reconstruct_stale_tr_with_pr(df, verbose=False)
        captured = capsys.readouterr()

        pd.testing.assert_frame_equal(result, df_copy)
        assert 'WARNING' in captured.out
        assert 'no clean day after last_stale' in captured.out

    def test_sparse_stale_throughout_is_not_reconstructed(self):
        """TOPIX-like: 전체 기간 산발적 stale (density 높음). gate 거부 → 재구성 안 함."""
        n = 200
        idx = pd.date_range('2000-01-03', periods=n, freq='B')
        rng = np.random.default_rng(0)
        pr_vals = 100.0 * np.cumprod(1 + rng.normal(0.0005, 0.01, size=n))
        tr_vals = 500.0 * np.cumprod(1 + rng.normal(0.0005, 0.01, size=n))
        # 20일마다 TR을 전날과 같게 → 산발적 stale ~5%
        for i in range(20, n, 20):
            tr_vals[i] = tr_vals[i - 1]
        df = _make_index_df({
            ('SPARSE', 'Sparse Stale'): {
                'FG_PRICE': pd.Series(pr_vals, index=idx),
                'FG_TOTAL_RET_IDX': pd.Series(tr_vals, index=idx),
            }
        })
        df_copy = df.copy(deep=True)

        result = reconstruct_stale_tr_with_pr(df, verbose=False)

        pd.testing.assert_frame_equal(result, df_copy)

    def test_dense_early_stale_with_monthly_updates(self):
        """사용자 시나리오: 초기 100일 TR이 20일마다만 변화, 이후 100일 매일 변화."""
        n = 200
        idx = pd.date_range('2000-01-03', periods=n, freq='B')
        rng = np.random.default_rng(1)
        pr_vals = 100.0 * np.cumprod(1 + rng.normal(0.0005, 0.01, size=n))

        tr_vals = np.empty(n)
        last_tr = 500.0
        for i in range(n):
            if i < 100:
                if i > 0 and i % 20 == 0:
                    last_tr *= (1 + rng.normal(0.01, 0.02))
                tr_vals[i] = last_tr
            else:
                last_tr *= (1 + rng.normal(0.0005, 0.01))
                tr_vals[i] = last_tr

        df = _make_index_df({
            ('EARLY', 'Early Stale'): {
                'FG_PRICE': pd.Series(pr_vals, index=idx),
                'FG_TOTAL_RET_IDX': pd.Series(tr_vals, index=idx),
            }
        })

        result = reconstruct_stale_tr_with_pr(df, verbose=False)

        new_tr = result[('EARLY', 'Early Stale', 'FG_TOTAL_RET_IDX')]
        orig_tr = df[('EARLY', 'Early Stale', 'FG_TOTAL_RET_IDX')]

        # 후기 50일 원본 유지 (anchor 이후)
        pd.testing.assert_series_equal(new_tr.iloc[-50:], orig_tr.iloc[-50:], check_names=False)
        # 초기 30일 변경됨
        assert not np.allclose(new_tr.iloc[:30].values, orig_tr.iloc[:30].values)

    def test_gate_threshold_parameter_respected(self):
        """threshold를 매우 엄격하게 설정하면 sparse-stale도 재구성 대상이 됨."""
        n = 200
        idx = pd.date_range('2000-01-03', periods=n, freq='B')
        rng = np.random.default_rng(2)
        pr_vals = 100.0 * np.cumprod(1 + rng.normal(0.0005, 0.01, size=n))
        tr_vals = 500.0 * np.cumprod(1 + rng.normal(0.0005, 0.01, size=n))
        for i in range(20, n, 20):
            tr_vals[i] = tr_vals[i - 1]

        df = _make_index_df({
            ('X', 'SparseX'): {
                'FG_PRICE': pd.Series(pr_vals, index=idx),
                'FG_TOTAL_RET_IDX': pd.Series(tr_vals, index=idx),
            }
        })

        # 기본 threshold=0.5 로는 건드리지 않음 (density ~95% > 0.5)
        result_default = reconstruct_stale_tr_with_pr(df, verbose=False)
        pd.testing.assert_frame_equal(result_default, df)

        # threshold=0.99 로 올리면 gate 통과, 재구성 발생
        result_strict = reconstruct_stale_tr_with_pr(df, threshold=0.99, verbose=False)
        new_tr = result_strict[('X', 'SparseX', 'FG_TOTAL_RET_IDX')]
        orig_tr = df[('X', 'SparseX', 'FG_TOTAL_RET_IDX')]
        assert not new_tr.equals(orig_tr)

    def test_max_gap_in_block_parameter_respected(self):
        """두 개의 stale block이 10일 clean 구간으로 분리된 데이터로 max_gap 동작 확인."""
        n = 160
        idx = pd.date_range('2000-01-03', periods=n, freq='B')
        rng = np.random.default_rng(1)
        pr_vals = 100.0 * np.cumprod(1 + rng.normal(0.0005, 0.01, size=n))
        tr_vals = np.empty(n)
        # 0: 초기값 / 1-49: frozen / 50-59: daily moving / 60-109: frozen again / 110-159: daily moving
        tr_vals[0] = 500.0
        for i in range(1, 50):
            tr_vals[i] = 500.0  # frozen
        for i in range(50, 60):
            tr_vals[i] = tr_vals[i - 1] * (1 + rng.normal(0.0005, 0.01))  # moving
        for i in range(60, 110):
            tr_vals[i] = tr_vals[59]  # frozen again
        for i in range(110, n):
            tr_vals[i] = tr_vals[i - 1] * (1 + rng.normal(0.0005, 0.01))  # moving

        df = _make_index_df({
            ('TWO', 'Two-Block Stale'): {
                'FG_PRICE': pd.Series(pr_vals, index=idx),
                'FG_TOTAL_RET_IDX': pd.Series(tr_vals, index=idx),
            }
        })
        orig_tr = df[('TWO', 'Two-Block Stale', 'FG_TOTAL_RET_IDX')]

        # max_gap=5: 첫 block만 잡히고 끝 (49일에서 60일까지의 10일 gap이 block 끊음)
        result_small = reconstruct_stale_tr_with_pr(df, max_gap_in_block=5, verbose=False)
        new_tr_small = result_small[('TWO', 'Two-Block Stale', 'FG_TOTAL_RET_IDX')]
        changed_small = (new_tr_small != orig_tr).sum()

        # max_gap=15: 10일 gap 허용 → 두 block이 하나로 합쳐짐
        result_big = reconstruct_stale_tr_with_pr(df, max_gap_in_block=15, verbose=False)
        new_tr_big = result_big[('TWO', 'Two-Block Stale', 'FG_TOTAL_RET_IDX')]
        changed_big = (new_tr_big != orig_tr).sum()

        assert changed_small > 0
        assert changed_big > changed_small

    def test_two_level_columns_supported(self):
        pr, tr = _make_frozen_then_clean(n_frozen=80, n_clean=20)
        sub = pd.DataFrame({'FG_PRICE': pr, 'FG_TOTAL_RET_IDX': tr})
        sub.columns = pd.MultiIndex.from_product([['T1'], sub.columns], names=['ticker', 'item_name'])

        result = reconstruct_stale_tr_with_pr(sub, verbose=False)

        new_tr = result[('T1', 'FG_TOTAL_RET_IDX')]
        scale = tr.iloc[80] / pr.iloc[80]
        assert np.allclose(new_tr.iloc[:80].values, (pr.iloc[:80] * scale).values, atol=1e-9)
        assert new_tr.iloc[80] == tr.iloc[80]

    def test_verbose_emits_per_ticker_lines(self, capsys):
        pr, tr = _make_frozen_then_clean(n_frozen=80, n_clean=20)
        df = _make_index_df({('T1', 'FrozenIdx'): {'FG_PRICE': pr, 'FG_TOTAL_RET_IDX': tr}})

        _ = reconstruct_stale_tr_with_pr(df, verbose=True)
        out = capsys.readouterr().out

        assert 'T1' in out
        assert 'FrozenIdx' in out
        assert 'last_stale=' in out
        assert 'anchor=' in out
        assert 'Done:' in out

    def test_non_verbose_hides_per_ticker_but_keeps_summary(self, capsys):
        pr, tr = _make_frozen_then_clean(n_frozen=80, n_clean=20)
        df = _make_index_df({('T1', 'FrozenIdx'): {'FG_PRICE': pr, 'FG_TOTAL_RET_IDX': tr}})

        _ = reconstruct_stale_tr_with_pr(df, verbose=False)
        out = capsys.readouterr().out

        assert 'last_stale=' not in out    # per-ticker detail suppressed
        assert 'Done:' in out              # summary still present
