"""KODEX 200 당일종가 + SPY 전일종가×당일 USDKRW(KRW 환산) 시계열, 2003년~.

원격 AI_Quant quantdb(ai_ready.etf_timeseries / ai_ready.fx)에 접속.
DB 접근은 topquant_ksk.db.QuantDB 가 담당 (cloudflared 터널 + CF Access 서비스토큰).
시크릿(DB_USER/DB_PASSWORD/CF_ACCESS_CLIENT_ID/CF_ACCESS_CLIENT_SECRET)은 .env 에서 주입.

컬럼:
  date               : KR 거래일 (KODEX 200 거래일 기준)
  kodex_close_krw    : KODEX 200(069500.KS) 당일 종가 (adj_close_pr, KRW)
  spy_prev_close_usd : SPY 전일 종가 = 해당 KR일자보다 이전(<date) 마지막 SPY 종가 (USD)
  usdkrw             : 당일 SMBS 서울 15:30 종가 (KRW/USD; 없으면 직전 거래일)
  spy_prev_close_krw : spy_prev_close_usd × usdkrw
"""
import argparse
import os

import pandas as pd

from topquant_ksk.db import QuantDB

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))


def _require_env(name):
    val = os.environ.get(name)
    if not val:
        raise RuntimeError(
            f"환경변수 {name} 가 없습니다. repo 루트 .env 에 설정하거나 셸 환경변수로 주입하세요. "
            f"(.env 사용 시 python-dotenv 필요: pip install topquant-ksk[db])"
        )
    return val


def build(db, since="2003-01-01"):
    def q(sql, params=None):
        df = db.read_sql(sql, params)
        df["date"] = pd.to_datetime(df["date"])
        return df

    kodex = q("SELECT date, adj_close_pr AS kodex_close_krw FROM ai_ready.etf_timeseries "
              "WHERE ticker='069500.KS' AND date >= :since ORDER BY date", {"since": since})
    spy = q("SELECT date, adj_close_pr AS spy_prev_close_usd FROM ai_ready.etf_timeseries "
            "WHERE ticker='SPY' ORDER BY date")
    fx = q("SELECT date, usdkrw_smbs_close AS usdkrw FROM ai_ready.fx "
           "WHERE usdkrw_smbs_close IS NOT NULL ORDER BY date")

    # KR 거래일(KODEX)에 정렬. SPY는 strictly 이전(<date) 마지막 종가 = 전일종가.
    df = pd.merge_asof(kodex, spy, on="date", direction="backward", allow_exact_matches=False)
    # 당일 SMBS 환율(없으면 직전 거래일).
    df = pd.merge_asof(df, fx, on="date", direction="backward", allow_exact_matches=True)
    df["spy_prev_close_krw"] = df["spy_prev_close_usd"] * df["usdkrw"]
    return df[["date", "kodex_close_krw", "spy_prev_close_usd", "usdkrw", "spy_prev_close_krw"]]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2003-01-01")
    ap.add_argument("--out", default=None, help="CSV 경로 (없으면 head/tail 미리보기)")
    a = ap.parse_args()

    with QuantDB(db_user=_require_env("DB_USER"), db_password=_require_env("DB_PASSWORD")) as db:
        df = build(db, a.since)

    if a.out:
        df.to_csv(a.out, index=False, encoding="utf-8-sig")
        print("wrote %d rows -> %s (%s..%s)"
              % (len(df), a.out, df.date.min().date(), df.date.max().date()))
    else:
        with pd.option_context("display.width", 160):
            print(df.head().to_string(index=False))
            print("...")
            print(df.tail().to_string(index=False))
        print("rows=%d span=%s..%s" % (len(df), df.date.min().date(), df.date.max().date()))


if __name__ == "__main__":
    main()
