"""QuantDB 원격 통합 테스트.

실제 .env 설정 + cloudflared 터널 + 원격 quantdb 가 있어야 의미가 있다.
설정/접속이 불가능하면 자동 skip 하므로 CI(타인 환경)에서도 깨지지 않는다.

로컬에서 실행:
    set PYTHONIOENCODING=utf-8
    python -m pytest tests/test_remote_db.py -v -rs
"""
import os

from dotenv import load_dotenv
import pytest

from topquant_ksk.db import QuantDB


def _connect_or_skip():
    # .env 로드 후 credential 이 있으면 QuantDB 구성. dbname/hostname 은 기본값.
    load_dotenv(override=True)
    user, password = os.environ.get("DB_USER"), os.environ.get("DB_PASSWORD")
    if not (user and password):
        pytest.skip(".env 의 DB_USER/DB_PASSWORD 미설정 — 통합 테스트 skip")
    return QuantDB(db_user=user, db_password=password)


def test_quantdb_connects_and_selects_one():
    db = _connect_or_skip()
    try:
        with db as d:
            out = d.read_sql("SELECT 1 AS x")
    except Exception as e:  # noqa: BLE001 — 터널/네트워크/DB 불가 시 환경 문제로 skip
        pytest.skip(f"원격 quantdb 접속 불가 (터널/네트워크/인증): {e}")
    assert out.iloc[0]["x"] == 1


def test_quantdb_reads_ai_ready_etf_timeseries():
    db = _connect_or_skip()
    try:
        with db as d:
            out = d.read_sql(
                "SELECT date, adj_close_pr FROM ai_ready.etf_timeseries "
                "WHERE ticker = :t ORDER BY date DESC LIMIT 3",
                {"t": "069500.KS"},
            )
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"원격 quantdb 접속 불가: {e}")
    assert list(out.columns) == ["date", "adj_close_pr"]
    assert len(out) <= 3
