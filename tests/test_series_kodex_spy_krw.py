import importlib.util
import os
import pandas as pd

# 스크립트는 패키지가 아니라 repo 루트의 단일 파일 → 경로로 직접 로드
_SCRIPT = os.path.join(os.path.dirname(__file__), "..", "series_kodex_spy_krw_tunnel.py")
_spec = importlib.util.spec_from_file_location("series_kodex_spy_krw_tunnel", _SCRIPT)
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)


class _FakeDB:
    """sql 내용으로 어떤 시계열을 요청하는지 판별해 캔드 프레임 반환."""

    def __init__(self):
        self.calls = []

    def read_sql(self, sql, params=None):
        self.calls.append((sql, params))
        if "069500.KS" in sql:
            return pd.DataFrame({
                "date": ["2003-01-02", "2003-01-03"],
                "kodex_close_krw": [100.0, 110.0],
            })
        if "ticker='SPY'" in sql:
            return pd.DataFrame({
                "date": ["2003-01-01", "2003-01-02"],
                "spy_prev_close_usd": [50.0, 55.0],
            })
        if "ai_ready.fx" in sql:
            return pd.DataFrame({
                "date": ["2003-01-02", "2003-01-03"],
                "usdkrw": [1200.0, 1210.0],
            })
        raise AssertionError(f"예상치 못한 쿼리: {sql}")


def test_build_merges_and_derives_krw():
    db = _FakeDB()
    out = mod.build(db, since="2003-01-01")

    assert list(out.columns) == [
        "date", "kodex_close_krw", "spy_prev_close_usd", "usdkrw", "spy_prev_close_krw"
    ]
    # 첫 read_sql(KODEX) 에 since 바인딩 전달
    assert db.calls[0][1] == {"since": "2003-01-01"}

    row = out[out["date"] == pd.Timestamp("2003-01-03")].iloc[0]
    # SPY 전일종가 = strictly < 01-03 의 마지막 = 01-02 의 55
    assert row["spy_prev_close_usd"] == 55.0
    # 당일 환율 = <= 01-03 = 01-03 의 1210
    assert row["usdkrw"] == 1210.0
    assert row["spy_prev_close_krw"] == 55.0 * 1210.0
    assert row["kodex_close_krw"] == 110.0


def test_build_spy_is_strictly_previous():
    db = _FakeDB()
    out = mod.build(db, since="2003-01-01")
    row = out[out["date"] == pd.Timestamp("2003-01-02")].iloc[0]
    # 01-02 의 SPY 전일종가 = strictly < 01-02 = 01-01 의 50 (01-02 자기 자신 아님)
    assert row["spy_prev_close_usd"] == 50.0
