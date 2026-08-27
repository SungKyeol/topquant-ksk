import glob
import os

import pandas as pd
import pytest

import topquant_ksk.db.quantdb as qd
from topquant_ksk.db.quantdb import (
    EtfUniversePanel,
    _make_dsn,
    _service_token_env,
    _tunnel_cmd,
    DEFAULT_DBNAME,
    DEFAULT_HOSTNAME,
    DEFAULT_LOCAL_PORT,
    DEFAULT_STATEMENT_TIMEOUT,
    QuantDB,
)

# QuantDB 관련 env 키 — 단위 테스트는 OS 환경/.env 오염 없이 hermetic 해야 한다.
_QUANTDB_ENV_KEYS = (
    "DB_USER", "DB_PASSWORD", "DB_NAME", "TUNNEL_HOSTNAME", "TUNNEL_PORT",
    "CLOUDFLARED_BIN", "CF_ACCESS_CLIENT_ID", "CF_ACCESS_CLIENT_SECRET",
    "TUNNEL_SERVICE_TOKEN_ID", "TUNNEL_SERVICE_TOKEN_SECRET",
)


@pytest.fixture(autouse=True)
def _clean_env():
    """각 테스트 전후로 QuantDB 관련 env 키를 제거 (OS 환경/.env 영향 차단, 누수 방지)."""
    def _clear():
        for k in _QUANTDB_ENV_KEYS:
            os.environ.pop(k, None)
    _clear()
    yield
    _clear()


def _qdb(**kw):
    """필수 4개 필드를 채운 QuantDB. 테스트별로 일부만 override."""
    base = dict(db_user="u", db_password="p", dbname="quantdb", hostname="shquantdb.alphawaves.vip")
    base.update(kw)
    return QuantDB(**base)


def test_quantdb_exported_from_db_package():
    from topquant_ksk.db import QuantDB as ExportedQuantDB
    from topquant_ksk.db.quantdb import QuantDB as DirectQuantDB
    assert ExportedQuantDB is DirectQuantDB


class TestMakeDsn:
    def test_basic_dsn(self):
        dsn = _make_dsn("u", "p", 15432, "quantdb", statement_timeout=None)
        assert dsn == "postgresql://u:p@127.0.0.1:15432/quantdb"

    def test_password_special_chars_quoted(self):
        # '@' 가 들어간 비밀번호도 URL-safe 하게 인코딩되어야 함
        dsn = _make_dsn("shtopquant", "pw@1", 15432, "quantdb")
        assert "pw%401" in dsn
        assert "@127.0.0.1" in dsn  # host 구분자는 1개만

    def test_statement_timeout_default_is_none(self):
        """기본값은 상한 없음 — 상한의 소유자는 서버 conf 다 (ADR-0050).

        DSN 의 libpq `options` 는 `source=client` 라 서버 전역값을 **덮는다**. 여기에 기본값을
        박으면 이 라이브러리로 붙는 모든 소비자가 서버 정책에 구멍을 낸다.
        """
        assert DEFAULT_STATEMENT_TIMEOUT is None
        assert "options" not in _make_dsn("u", "p", 15432, "quantdb")

    def test_statement_timeout_custom_seconds_to_ms(self):
        assert _make_dsn("u", "p", 15432, "quantdb", 60).endswith("=60000")

    def test_statement_timeout_none_omits_options(self):
        assert "options" not in _make_dsn("u", "p", 15432, "quantdb", None)

    def test_options_has_no_space(self):
        """공백이 든 `-c name=value` 는 connectorx 접속을 깨뜨린다 — `--name=value` 형태 유지."""
        dsn = _make_dsn("u", "p", 15432, "quantdb", 7200)
        opts = dsn.split("?options=", 1)[1]
        assert " " not in opts and "+" not in opts and "%20" not in opts
        assert opts.startswith("--")


class TestServiceTokenEnv:
    def test_both_present_returns_tunnel_vars(self):
        env = _service_token_env("cid", "csec")
        assert env == {
            "TUNNEL_SERVICE_TOKEN_ID": "cid",
            "TUNNEL_SERVICE_TOKEN_SECRET": "csec",
        }

    def test_missing_returns_empty(self):
        assert _service_token_env(None, None) == {}
        assert _service_token_env("cid", None) == {}
        assert _service_token_env(None, "csec") == {}


class TestTunnelCmd:
    def test_cmd_shape(self):
        cmd = _tunnel_cmd("cf.exe", "h.example", 15432)
        assert cmd == ["cf.exe", "access", "tcp", "--hostname", "h.example", "--url", "127.0.0.1:15432"]


class TestQuantDBInit:
    def test_defaults_applied(self):
        db = QuantDB("u", "p")                  # dbname/hostname 생략 → 기본값
        assert db.db_user == "u" and db.db_password == "p"
        assert db.dbname == DEFAULT_DBNAME == "quantdb"
        assert db.hostname == DEFAULT_HOSTNAME == "shquantdb.alphawaves.vip"
        assert db.local_port == DEFAULT_LOCAL_PORT == 15432
        assert db.cf_client_id is None and db.cf_client_secret is None
        assert db.cloudflared_bin is None       # 미지정 → 런타임 자동탐지

    def test_override_optionals(self):
        db = QuantDB(db_user="u", db_password="p", dbname="d", hostname="h",
                     local_port="25432", cf_client_id="cid", cf_client_secret="csec",
                     cloudflared_bin="/x/cf.exe")
        assert (db.dbname, db.hostname) == ("d", "h")
        assert db.local_port == 25432           # str → int 변환
        assert (db.cf_client_id, db.cf_client_secret, db.cloudflared_bin) == ("cid", "csec", "/x/cf.exe")

    def test_does_not_read_env(self, monkeypatch):
        # 클래스는 env 를 읽지 않는다 — env 가 달라도 기본값/인자만 사용
        monkeypatch.setenv("DB_NAME", "envdb")
        monkeypatch.setenv("TUNNEL_HOSTNAME", "env.host")
        db = QuantDB("u", "p")
        assert db.dbname == "quantdb" and db.hostname == "shquantdb.alphawaves.vip"  # env 무시

    def test_missing_credential_raises_typeerror(self):
        with pytest.raises(TypeError):
            QuantDB("u")               # db_password 누락 → 필수 인자

    def test_empty_credential_raises_valueerror(self):
        with pytest.raises(ValueError):
            QuantDB(db_user="", db_password="p")
        with pytest.raises(ValueError):
            QuantDB(db_user="u", db_password="")

    def test_explicit_none_dbname_raises_valueerror(self):
        with pytest.raises(ValueError) as ei:
            QuantDB(db_user="u", db_password="p", dbname=None, hostname=None)
        assert "dbname" in str(ei.value) and "hostname" in str(ei.value)


class TestEngineProperty:
    def test_engine_outside_context_raises(self):
        db = _qdb()
        with pytest.raises(RuntimeError):
            _ = db.engine


class _FakeResult:
    def __init__(self, rows, keys):
        self._rows = rows
        self._keys = keys

    def fetchall(self):
        return self._rows

    def keys(self):
        return self._keys


class _FakeConn:
    def __init__(self, result):
        self.result = result
        self.executed = []

    def execute(self, stmt, params):
        self.executed.append((str(stmt), params))
        return self.result

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeEngine:
    def __init__(self, conn):
        self._conn = conn
        self.disposed = False

    def connect(self):
        return self._conn

    def dispose(self):
        self.disposed = True


class TestReadSql:
    def test_read_sql_builds_dataframe_and_passes_params(self):
        conn = _FakeConn(_FakeResult([(1, 2), (3, 4)], ["a", "b"]))
        db = _qdb()
        db._engine = _FakeEngine(conn)
        out = db.read_sql("SELECT a, b FROM t WHERE x >= :x", {"x": 10}, verbose=False)
        assert list(out.columns) == ["a", "b"]
        assert out.iloc[1]["b"] == 4
        assert conn.executed[0][1] == {"x": 10}  # 바인딩 파라미터 전달 확인

    def test_read_sql_none_params(self):
        conn = _FakeConn(_FakeResult([(5,)], ["v"]))
        db = _qdb()
        db._engine = _FakeEngine(conn)
        out = db.read_sql("SELECT v FROM t", verbose=False)
        assert conn.executed[0][1] == {}
        assert out.iloc[0]["v"] == 5

    def test_read_sql_prints_by_default(self, capsys):
        conn = _FakeConn(_FakeResult([(42,)], ["answer"]))
        db = _qdb()
        db._engine = _FakeEngine(conn)
        db.read_sql("SELECT answer")                 # verbose 기본 True → 자동 print
        out = capsys.readouterr().out
        assert "answer" in out and "42" in out

    def test_read_sql_verbose_false_silent(self, capsys):
        conn = _FakeConn(_FakeResult([(42,)], ["answer"]))
        db = _qdb()
        db._engine = _FakeEngine(conn)
        db.read_sql("SELECT answer", verbose=False)
        assert capsys.readouterr().out == ""


class TestListTables:
    _KEYS = ["schema", "name", "type", "columns"]
    _ROW = ("ai_ready", "etf_daily", "view", "ticker, date, adj_close_pr, adj_close_tr, currency")

    def test_returns_dataframe_and_passes_schema(self):
        conn = _FakeConn(_FakeResult([self._ROW], self._KEYS))
        db = _qdb()
        db._engine = _FakeEngine(conn)
        out = db.list_tables(schema="ai_ready", verbose=False)
        assert list(out.columns) == self._KEYS
        assert out.iloc[0]["name"] == "etf_daily" and out.iloc[0]["type"] == "view"
        assert "ticker" in out.iloc[0]["columns"] and "currency" in out.iloc[0]["columns"]
        assert conn.executed[0][1] == {"schema": "ai_ready"}   # schema 바인딩 전달

    def test_schema_none_default(self):
        conn = _FakeConn(_FakeResult([], self._KEYS))
        db = _qdb()
        db._engine = _FakeEngine(conn)
        db.list_tables(verbose=False)
        assert conn.executed[0][1] == {"schema": None}

    def test_default_prints_full_columns(self, capsys):
        conn = _FakeConn(_FakeResult([self._ROW], self._KEYS))
        db = _qdb()
        db._engine = _FakeEngine(conn)
        db.list_tables()                              # verbose 기본 True → 자동 print
        out = capsys.readouterr().out
        assert "etf_daily" in out
        assert "adj_close_pr" in out and "currency" in out   # 컬럼 잘리지 않고 전부 출력

    def test_verbose_false_silent(self, capsys):
        conn = _FakeConn(_FakeResult([self._ROW], self._KEYS))
        db = _qdb()
        db._engine = _FakeEngine(conn)
        db.list_tables(verbose=False)
        assert capsys.readouterr().out == ""


class TestIsDateOnly:
    def test_date_object_is_date_only(self):
        import datetime
        assert qd._is_date_only(datetime.date(2010, 4, 6)) is True

    def test_datetime_object_not_date_only(self):
        import datetime
        assert qd._is_date_only(datetime.datetime(2010, 4, 6, 15, 30)) is False
        # 자정 datetime 도 '명시 시각' 으로 취급 (정확 경계 유지)
        assert qd._is_date_only(datetime.datetime(2010, 4, 6)) is False

    def test_date_string_is_date_only(self):
        assert qd._is_date_only("2010-04-06") is True
        assert qd._is_date_only("20100406") is True

    def test_string_with_time_not_date_only(self):
        assert qd._is_date_only("2010-04-06 15:30") is False
        assert qd._is_date_only("2010-04-06T09:00:00") is False


class TestFetchTimeseries:
    _COLS = pd.DataFrame({
        "column_name": ["ticker", "name", "isin", "sec_type", "ts", "close", "volume", "tradingitemid"],
        "typcat": ["S", "S", "S", "S", "D", "N", "N", "N"],   # S=string D=datetime N=numeric
    })
    _LONG = pd.DataFrame({
        "ts": ["2026-06-23", "2026-06-23", "2026-06-24", "2026-06-24"],
        "ticker": ["A", "B", "A", "B"],
        "name": ["aa", "bb", "aa", "bb"],
        "isin": ["I1", "I2", "I1", "I2"],
        "tradingitemid": [11, 22, 11, 22],     # identity 에 포함 → long 에도 있어야 pivot 된다
        "close": [1.0, 2.0, 3.0, 4.0],
        "volume": [10.0, 20.0, 30.0, 40.0],
    })

    def _db(self, capture, cols=None, long=None):
        db = _qdb()
        cols = self._COLS if cols is None else cols
        long = self._LONG if long is None else long

        def fake_read_sql(sql, params=None, verbose=True):
            capture.append((sql, params))                  # read_sql 은 이제 컬럼감지(pg_attribute)에만
            return cols.copy()

        def fake_bulk_read(sql):
            capture.append((sql, None))                    # 데이터 fetch 는 connectorx → _bulk_read (pandas 반환)
            return long.copy()

        db.read_sql = fake_read_sql
        db._bulk_read = fake_bulk_read
        return db

    def _data_sql(self, capture):
        return next(s for s, _ in capture if "pg_attribute" not in s)

    def test_pivots_to_multiindex(self):
        out = self._db([]).fetch_timeseries("spot_kr_5min", ids=["A", "B"], start="2026-06-23", verbose=False)
        # tradingitemid 는 맨 뒤 — 앞에 오면 identity[0](=기본 엔티티)이 바뀌어 기존 호출이 0행이 된다.
        assert list(out.columns.names) == ["item", "ticker", "name", "isin", "tradingitemid"]
        assert out.index.name == "ts"
        assert ("close", "A", "aa", "I1", 11) in out.columns
        assert ("volume", "B", "bb", "I2", 22) in out.columns
        assert out.loc[pd.Timestamp("2026-06-24"), ("close", "A", "aa", "I1", 11)] == 3.0

    def test_value_detection_drops_id_and_text(self):
        cap = []
        out = self._db(cap).fetch_timeseries("spot_kr_5min", ids=["A"], verbose=False)
        sel = self._data_sql(cap).split("FROM")[0]
        assert "close" in sel and "volume" in sel          # numeric 측정값
        assert "sec_type" not in sel                       # text 는 값도 식별자도 아님 → 통째로 제외
        # tradingitemid 는 SELECT 에 있다 (identity). '값'이 아니어야 한다는 게 요점이라 item 레벨로 본다.
        assert "tradingitemid" in sel
        assert set(out.columns.get_level_values("item")) == {"close", "volume"}

    def test_entity_and_time_filter_in_sql(self):
        cap = []
        self._db(cap).fetch_timeseries("spot_kr_5min", ids=["A"], start="2026-01-01", end="2026-06-30", verbose=False)
        sql = self._data_sql(cap)
        # connectorx 는 :name 바인드 미지원 → 값은 _sql_lit 로 인라인 (작은따옴표 escape)
        # 날짜-only end 는 ts(timestamp) 패널에서 그날 끝까지 inclusive → 다음날 자정 미만
        # (구 'ts <= 날짜' 는 자정 경계라 그날 인트라데이 봉이 통째로 빠지던 버그)
        assert "ticker IN ('A')" in sql and "ts >= '2026-01-01'" in sql
        assert "ts < ('2026-06-30'::date + 1)" in sql

    def test_intraday_ts_projected_as_kst(self):
        # ts(timestamptz) 패널은 KST wall-clock 으로 투영해야 한다 — connectorx 가 timestamptz 를
        # naive-UTC 로 떨궈 09:05 KST 가 00:05 로 보이던 문제. (일봉 date 패널엔 적용 안 함.)
        cap = []
        self._db(cap).fetch_timeseries("stock_kr_5min", ids=["A"], start="2010-04-06", verbose=False)
        sel = self._data_sql(cap).split("FROM")[0]
        assert "ts AT TIME ZONE 'Asia/Seoul'" in sel and "AS ts" in sel

    def test_daily_date_panel_not_tz_projected(self):
        cols = pd.DataFrame({
            "column_name": ["ticker", "name", "isin", "date", "close"],
            "typcat": ["S", "S", "S", "D", "N"],
        })
        long = pd.DataFrame({"date": ["2026-06-30"], "ticker": ["A"], "name": ["aa"],
                             "isin": ["I1"], "close": [1.0]})
        cap = []
        self._db(cap, cols=cols, long=long).fetch_timeseries(
            "stock_kr_daily", ids=["A"], verbose=False)
        sel = self._data_sql(cap).split("FROM")[0]
        assert "AT TIME ZONE" not in sel               # date 패널(tz 개념 없음)은 변환하지 않음

    def test_intraday_date_only_end_includes_whole_day(self):
        # 인트라데이(ts) 패널: 단일일 조회 end=start=날짜 → 그날 봉 전체 포함해야 한다.
        cap = []
        self._db(cap).fetch_timeseries("stock_kr_5min", ids=["A"],
                                       start="2010-04-06", end="2010-04-06", verbose=False)
        sql = self._data_sql(cap)
        assert "ts >= '2010-04-06'" in sql
        assert "ts < ('2010-04-06'::date + 1)" in sql      # 다음날 자정 미만 = 그날 inclusive
        assert "ts <= '2010-04-06'" not in sql             # 버그였던 자정 상한이 아니어야

    def test_intraday_end_with_explicit_time_kept_exact(self):
        # end 에 시각이 있으면(명시 timestamp) 정확 경계로 <= 그대로 둔다.
        cap = []
        self._db(cap).fetch_timeseries("stock_kr_5min", ids=["A"],
                                       start="2010-04-06", end="2010-04-06 15:30", verbose=False)
        sql = self._data_sql(cap)
        assert "ts <= '2010-04-06 15:30'" in sql
        assert "::date + 1" not in sql

    def test_daily_date_panel_end_stays_inclusive(self):
        # 일봉(date) 패널은 date<=date 가 정상 → end-of-day 변환을 적용하지 않는다.
        cols = pd.DataFrame({
            "column_name": ["ticker", "name", "isin", "date", "close"],
            "typcat": ["S", "S", "S", "D", "N"],
        })
        long = pd.DataFrame({"date": ["2026-06-30"], "ticker": ["A"], "name": ["aa"],
                             "isin": ["I1"], "close": [1.0]})
        cap = []
        self._db(cap, cols=cols, long=long).fetch_timeseries(
            "stock_kr_daily", ids=["A"], start="2026-01-01", end="2026-06-30", verbose=False)
        sql = self._data_sql(cap)
        assert "date <= '2026-06-30'" in sql               # date 패널은 그대로 inclusive
        assert "::date + 1" not in sql

    def test_fields_override(self):
        cap = []
        self._db(cap).fetch_timeseries("spot_kr_5min", fields=["close"], ids=["A"], verbose=False)
        sel = self._data_sql(cap).split("FROM")[0]
        assert "close" in sel and "volume" not in sel

    def test_view_prefix_stripped_and_schema_passed(self):
        cap = []
        self._db(cap).fetch_timeseries("ai_ready.spot_kr_5min", ids=["A"], verbose=False)
        # pg_catalog(컬럼) 쿼리에 schema/table 분리 전달
        cols_params = next(p for s, p in cap if "pg_attribute" in s)
        assert cols_params == {"s": "ai_ready", "t": "spot_kr_5min"}
        assert "FROM ai_ready.spot_kr_5min" in self._data_sql(cap)

    def test_unfiltered_warns(self):
        with pytest.warns(UserWarning, match="전체 fetch"):
            self._db([]).fetch_timeseries("spot_kr_5min", verbose=False)

    def test_no_time_col_raises(self):
        cols = pd.DataFrame({"column_name": ["ticker", "name"], "typcat": ["S", "S"]})
        db = _qdb()
        db.read_sql = lambda sql, params=None, verbose=True: cols.copy()
        with pytest.raises(ValueError, match="시간 컬럼"):
            db.fetch_timeseries("id_map", verbose=False)

    def test_iso_code_dropped_when_ticker_present(self):
        cols = pd.DataFrame({
            "column_name": ["ticker", "name", "isin", "iso_code", "date", "close"],
            "typcat": ["S", "S", "S", "S", "D", "N"],
        })
        long = pd.DataFrame({"date": ["2026-01-01"], "ticker": ["A"], "name": ["aa"], "isin": ["I1"], "close": [1.0]})
        cap = []
        out = self._db(cap, cols=cols, long=long).fetch_timeseries("prices_daily_krw", ids=["A"], verbose=False)
        assert list(out.columns.names) == ["item", "ticker", "name", "isin"]   # iso_code 식별자 아님 → 제외
        assert "iso_code" not in self._data_sql(cap)

    def test_fx_style_iso_code_entity(self):
        cols = pd.DataFrame({
            "column_name": ["source", "currencyid", "iso_code", "currency_name", "date", "per_usd"],
            "typcat": ["S", "N", "S", "S", "D", "N"],
        })
        long = pd.DataFrame({"date": ["2026-01-01", "2026-01-02"], "iso_code": ["KRW", "KRW"],
                             "currency_name": ["won", "won"], "per_usd": [1300.0, 1310.0]})
        cap = []
        out = self._db(cap, cols=cols, long=long).fetch_timeseries("fx_daily", ids=["KRW"], verbose=False)
        assert list(out.columns.names) == ["item", "iso_code", "currency_name"]   # ticker 없음 → iso_code 그룹
        sql = self._data_sql(cap)
        assert "iso_code IN ('KRW')" in sql                                       # entity = iso_code (인라인 리터럴)
        assert "per_usd" in sql and "currencyid" not in sql.split("FROM")[0]      # *id/text drop

    def test_cache_miss_then_hit(self, tmp_path, monkeypatch):
        # 1차 호출: DB 조회 + pkl 저장. 2차 동일 호출: read_sql 미호출(캐시 히트) + 동일 df.
        monkeypatch.chdir(tmp_path)
        cap = []
        db = self._db(cap)
        out1 = db.fetch_timeseries("spot_kr_5min", ids=["A", "B"], start="2026-06-23",
                                   save_and_reload_pickle_cache=True, verbose=False)
        assert len(cap) > 0                                    # 1차는 조회함
        assert len(glob.glob("pickle_cache/*.pkl")) == 1       # 저장됨
        cap.clear()
        out2 = db.fetch_timeseries("spot_kr_5min", ids=["A", "B"], start="2026-06-23",
                                   save_and_reload_pickle_cache=True, verbose=False)
        assert cap == []                                       # 2차는 read_sql 미호출 = 히트
        pd.testing.assert_frame_equal(out1, out2)

    def test_cache_key_varies_by_params(self, tmp_path, monkeypatch):
        # tickers 가 다르면 키가 달라 거짓 히트 없이 각각 별도 pkl 생성.
        monkeypatch.chdir(tmp_path)
        db = self._db([])
        db.fetch_timeseries("spot_kr_5min", ids=["A"], save_and_reload_pickle_cache=True, verbose=False)
        db.fetch_timeseries("spot_kr_5min", ids=["B"], save_and_reload_pickle_cache=True, verbose=False)
        assert len(glob.glob("pickle_cache/*.pkl")) == 2       # 키 분리 → 2개 (거짓 히트면 1개)

    def test_empty_result_not_cached(self, tmp_path, monkeypatch):
        # 0행이면 pkl 을 저장하지 않는다 (transient empty 가 하루 고정되는 것 방지).
        monkeypatch.chdir(tmp_path)
        empty = self._LONG.iloc[0:0]
        db = self._db([], long=empty)
        out = db.fetch_timeseries("spot_kr_5min", ids=["A"], save_and_reload_pickle_cache=True, verbose=False)
        assert out.empty
        assert glob.glob("pickle_cache/*.pkl") == []           # 캐시 안 됨

    def test_broad_cleanup_removes_old_date_file(self, tmp_path, monkeypatch):
        # 같은 relation 의 당일 아닌 캐시는 호출 시 broad cleanup 으로 삭제, 당일 파일은 유지.
        monkeypatch.chdir(tmp_path)
        os.makedirs("pickle_cache", exist_ok=True)
        stale = os.path.join("pickle_cache", "ai_ready_spot_kr_5min_deadbeef0000_20200101.pkl")
        with open(stale, "wb") as f:
            f.write(b"x")
        db = self._db([])
        db.fetch_timeseries("spot_kr_5min", ids=["A"], save_and_reload_pickle_cache=True, verbose=False)
        assert not os.path.exists(stale)                       # 과거 날짜 파일 삭제
        assert len(glob.glob("pickle_cache/ai_ready_spot_kr_5min_*.pkl")) == 1   # 당일 파일만 남음

    def test_cache_hit_silent_when_verbose_false(self, tmp_path, monkeypatch, capsys):
        # verbose=False 면 캐시 로드도 무음 (클래스 계약 유지).
        monkeypatch.chdir(tmp_path)
        db = self._db([])
        db.fetch_timeseries("spot_kr_5min", ids=["A"], save_and_reload_pickle_cache=True, verbose=False)
        capsys.readouterr()                                    # 1차 출력 비우기
        db.fetch_timeseries("spot_kr_5min", ids=["A"], save_and_reload_pickle_cache=True, verbose=False)
        assert capsys.readouterr().out == ""                   # 히트 시 무음

    def test_verbose_prints_panel_not_just_summary(self, capsys):
        # verbose=True 면 요약뿐 아니라 result(wide panel) 도 print (read_sql/list_tables 와 동일 계약).
        self._db([]).fetch_timeseries("spot_kr_5min", ids=["A"], start="2026-06-23", verbose=True)
        out = capsys.readouterr().out
        assert "aa" in out          # identity name='aa' 는 panel 컬럼에만 — 요약 줄엔 없음 → result 가 실제 print 됨

    def test_verbose_prints_fetch_timing(self, capsys):
        # verbose=True 면 fetch 시작시각 + 걸린시간 print (verbose=False 면 무음 — 클래스 계약).
        self._db([]).fetch_timeseries("spot_kr_5min", ids=["A"], start="2026-06-23", verbose=True)
        out = capsys.readouterr().out
        assert "시작" in out and "완료" in out

    def test_fetch_timing_silent_when_verbose_false(self, capsys):
        self._db([]).fetch_timeseries("spot_kr_5min", ids=["A"], start="2026-06-23", verbose=False)
        assert capsys.readouterr().out == ""


class _FakeProc:
    def __init__(self, pid=4321):
        self.pid = pid


class TestFilterBy:
    """ids 를 어느 컬럼에 걸지 고르는 filter_by. WHERE 절만 바꾸고 피벗축은 건드리지 않는다."""

    _COLS = pd.DataFrame({
        "column_name": ["ticker", "name", "isin", "tradingitemid", "iso_code", "date", "close_pr"],
        "typcat": ["S", "S", "S", "N", "S", "D", "N"],
    })
    _LONG = pd.DataFrame({"date": ["2026-01-02"], "ticker": ["AAPL"], "name": ["Apple"],
                          "isin": ["US0378331005"], "tradingitemid": [2590360], "close_pr": [1.0]})

    def _db(self, capture, long=None):
        db = _qdb()
        long = self._LONG if long is None else long
        db.read_sql = lambda sql, params=None, verbose=True: self._COLS.copy()

        def grab(sql):
            capture.append(sql)
            return long.copy()

        db._bulk_read = grab
        return db

    def _sql(self, cap):
        return next(x for x in cap if "pg_attribute" not in x)

    def test_default_entity_is_ticker(self):
        cap = []
        with pytest.warns(UserWarning, match="ticker 로 필터"):
            self._db(cap).fetch_timeseries("prices_daily_usd", ids=["AAPL"], verbose=False)
        assert "ticker IN ('AAPL')" in self._sql(cap)

    def test_filter_by_isin(self):
        cap = []
        self._db(cap).fetch_timeseries("prices_daily_usd", ids=["US0378331005"],
                                       filter_by="isin", verbose=False)
        sql = self._sql(cap)
        assert "isin IN ('US0378331005')" in sql
        assert "ticker IN" not in sql

    def test_filter_by_tradingitemid_accepts_ints(self):
        # _sql_lit 이 정수를 따옴표로 감싸도 Postgres 가 bigint 로 해석한다 (실DB 확인).
        cap = []
        self._db(cap).fetch_timeseries("prices_daily_usd", ids=[2590360],
                                       filter_by="tradingitemid", verbose=False)
        assert "tradingitemid IN ('2590360')" in self._sql(cap)

    def test_single_int_id_not_iterated(self):
        # ids=2590360 (스칼라) 이 자릿수별로 쪼개지면 안 된다.
        cap = []
        self._db(cap).fetch_timeseries("prices_daily_usd", ids=2590360,
                                       filter_by="tradingitemid", verbose=False)
        assert "tradingitemid IN ('2590360')" in self._sql(cap)

    def test_pivot_axis_unchanged_by_filter_by(self):
        out = self._db([]).fetch_timeseries("prices_daily_usd", ids=[2590360],
                                            filter_by="tradingitemid", verbose=False)
        assert list(out.columns.names) == ["item", "ticker", "name", "isin", "tradingitemid"]

    def test_unknown_column_raises_before_db_hit(self):
        cap = []
        with pytest.raises(ValueError, match="filter_by 컬럼 'sedol' 없음"):
            self._db(cap).fetch_timeseries("prices_daily_usd", ids=["AAPL"],
                                           filter_by="sedol", verbose=False)
        assert cap == []                                   # 데이터 조회 전에 막힌다

    def test_filter_by_is_keyword_only(self):
        # end 뒤 위치인자로 끼우면 save_and_reload_pickle_cache 가 밀린다 → * 로 원천 차단.
        with pytest.raises(TypeError, match="positional argument"):
            self._db([]).fetch_timeseries("prices_daily_usd", None, ["AAPL"], None, None, "isin")

    def test_cache_key_varies_by_filter_by(self, tmp_path, monkeypatch):
        # 같은 ids 를 다른 컬럼에 거는 두 호출은 결과가 전혀 다르다 — 캐시 키가 갈려야 한다.
        monkeypatch.chdir(tmp_path)
        db = self._db([])
        db.fetch_timeseries("prices_daily_usd", ids=["US0378331005"], filter_by="isin",
                            save_and_reload_pickle_cache=True, verbose=False)
        db.fetch_timeseries("prices_daily_usd", ids=["US0378331005"], filter_by="tradingitemid",
                            save_and_reload_pickle_cache=True, verbose=False)
        assert len(glob.glob("pickle_cache/*.pkl")) == 2

    def test_split_isin_warns(self):
        # 한 ISIN 이 두 (ticker, name) 으로 갈리면 한 종목이 컬럼 둘이 된다 — 피벗은 안 깨지므로 조용하다.
        long = pd.DataFrame({
            "date": ["2026-01-02", "2026-01-02"],
            "ticker": ["EQR", "VMRK"], "name": ["Equity Residential", "Vivmark Residential"],
            "isin": ["US29476L1070", "US29476L1070"], "tradingitemid": [2610085, 2016912246],
            "close_pr": [63.66, 64.03]})
        with pytest.warns(UserWarning, match=r"둘 이상의 \(ticker, name\)"):
            self._db([], long=long).fetch_timeseries(
                "prices_daily_usd", ids=[2610085, 2016912246],
                filter_by="tradingitemid", verbose=False)

    def test_no_split_isin_warning_when_clean(self):
        import warnings as _w
        with _w.catch_warnings():
            _w.simplefilter("error")
            self._db([]).fetch_timeseries("prices_daily_usd", ids=[2590360],
                                          filter_by="tradingitemid", verbose=False)


class TestEtfUniversePanel:
    """ETF 전 이력 구성종목 유니버스 → 패널 + 편입행렬.

    가격은 tid 목록으로 평면 조회하고, span 은 membership 에만 적용한다 (ADR-0005).
    """

    _PRICE_COLS = pd.DataFrame({
        "column_name": ["ticker", "name", "isin", "tradingitemid", "date", "close_pr"],
        "typcat": ["S", "S", "S", "N", "D", "N"]})
    _KR_COLS = pd.DataFrame({
        "column_name": ["ticker", "name", "isin", "date", "adj_close_pr"],
        "typcat": ["S", "S", "S", "D", "N"]})
    _NOID_COLS = pd.DataFrame({
        "column_name": ["etf_code", "date", "weight"], "typcat": ["S", "D", "N"]})

    _UNIVERSE = pd.DataFrame({"isin": ["US_A", "US_B", "US_C"]})
    _BRIDGE = pd.DataFrame({"tradingitemid": [100, 200, 201]})
    # US_B 는 시기에 따라 라인이 갈린다(200 -> 201). US_A 는 SPY/QQQ 양쪽에 있어 중복 행이 온다.
    _MEM = pd.DataFrame({
        "month_end": ["2020-01-31", "2020-01-31", "2020-02-29", "2020-01-31", "2020-02-29", "2020-02-29"],
        "isin":      ["US_A",       "US_A",       "US_A",       "US_B",       "US_B",       "US_C"],
        "tradingitemid": [100.0,    100.0,        100.0,        200.0,        201.0,        None],
        "via":       ["isin_span"] * 5 + [None]})
    _LONG = pd.DataFrame({
        "date": ["2020-01-31", "2020-02-28"], "ticker": ["AA", "AA"], "name": ["A Inc", "A Inc"],
        "isin": ["US_A", "US_A"], "tradingitemid": [100, 100], "close_pr": [1.0, 2.0]})

    def _db(self, cols=None, long=None, capture=None):
        db = _qdb()
        cols = self._PRICE_COLS if cols is None else cols
        long = self._LONG if long is None else long
        cap = capture if capture is not None else []

        def fake_read_sql(sql, params=None, verbose=True):
            cap.append((sql, params))
            if "pg_attribute" in sql:
                return cols.copy()
            if "LEFT JOIN" in sql:
                return self._MEM.copy()
            if "DISTINCT tradingitemid" in sql:
                return self._BRIDGE.copy()
            return self._UNIVERSE.copy()

        def fake_bulk(sql):
            cap.append((sql, None))
            return long.copy()

        db.read_sql = fake_read_sql
        db._bulk_read = fake_bulk
        return db

    def _call(self, db, **kw):
        kw.setdefault("verbose", False)
        with pytest.warns(UserWarning):        # 미해석 ISIN(US_C) 경고는 항상 뜬다
            return db.fetch_etf_universe_panel("prices_daily_usd", ["SPY-US"], **kw)

    def test_returns_namedtuple_with_panels_dict(self):
        out = self._call(self._db())
        assert isinstance(out, EtfUniversePanel)
        assert list(out.panels) == ["prices_daily_usd"]        # relation 1개여도 dict
        assert not out.panels["prices_daily_usd"].empty

    def test_uses_tradingitemid_when_available(self):
        cap = []
        self._call(self._db(capture=cap))
        assert any("DISTINCT tradingitemid" in q for q, _ in cap)          # 브리지를 탔고
        assert any("tradingitemid IN ('100'" in q for q, _ in cap)         # tid 로 걸었다
        assert not any("ticker IN" in q for q, _ in cap)                   # ticker 로는 안 걸었다

    def test_membership_is_boolean_and_era_split(self):
        out = self._call(self._db())
        m = out.membership
        assert list(m.columns.names) == ["isin", "tradingitemid"]
        assert m.dtypes.unique().tolist() == [bool]
        assert ("US_B", 200) in m.columns and ("US_B", 201) in m.columns
        jan, feb = pd.Timestamp("2020-01-31"), pd.Timestamp("2020-02-29")
        assert m.loc[jan, ("US_B", 200)] and not m.loc[feb, ("US_B", 200)]
        assert not m.loc[jan, ("US_B", 201)] and m.loc[feb, ("US_B", 201)]

    def test_duplicate_fund_rows_deduped(self):
        # US_A 가 SPY/QQQ 양쪽에서 와 2행이지만 컬럼은 하나여야 한다.
        out = self._call(self._db())
        assert list(out.membership.columns).count(("US_A", 100)) == 1

    def test_unresolved_isin_warns_and_is_excluded(self):
        out = self._call(self._db())
        assert "US_C" not in out.membership.columns.get_level_values("isin")

    def test_uncovered_is_derivable(self):
        out = self._call(self._db())
        covered = set(out.panels["prices_daily_usd"].columns.get_level_values("isin"))
        uncovered = set(out.membership.columns.get_level_values("isin")) - covered
        assert uncovered == {"US_B"}                            # 패널엔 US_A 만 있다

    def test_falls_back_to_isin_when_no_tradingitemid(self):
        db = self._db(cols=self._KR_COLS,
                      long=pd.DataFrame({"date": ["2020-01-31"], "ticker": ["A005930"],
                                         "name": ["삼성전자"], "isin": ["KR_X"],
                                         "adj_close_pr": [1.0]}))
        with pytest.warns(UserWarning):
            out = db.fetch_etf_universe_panel("spot_kr_daily", ["SPY-US"], verbose=False)
        assert list(out.panels) == ["spot_kr_daily"]

    def test_relation_without_any_id_raises(self):
        db = self._db(cols=self._NOID_COLS)
        with pytest.warns(UserWarning):
            with pytest.raises(ValueError, match="주소지정 불가"):
                db.fetch_etf_universe_panel("etf_kr_holdings_daily", ["SPY-US"], verbose=False)

    def test_empty_panel_warns(self):
        db = self._db(long=self._LONG.iloc[0:0])
        with pytest.warns(UserWarning, match="교집합이 없습니다"):
            db.fetch_etf_universe_panel("prices_daily_usd", ["SPY-US"], verbose=False)

    def test_empty_universe_raises_with_country_hint(self):
        db = self._db()
        db.read_sql = lambda sql, params=None, verbose=True: pd.DataFrame({"isin": []})
        with pytest.raises(ValueError, match="국가수식"):
            db.fetch_etf_universe_panel("prices_daily_usd", ["SPY"], verbose=False)

    def test_fields_dict_per_relation(self):
        seen = {}
        db = self._db()
        real = db.fetch_timeseries

        def spy(relation, fields=None, **kw):
            seen[relation] = fields
            return real(relation, fields=fields, **kw)

        db.fetch_timeseries = spy
        with pytest.warns(UserWarning):
            db.fetch_etf_universe_panel(["prices_daily_usd"], ["SPY-US"],
                                        fields={"prices_daily_usd": ["close_pr"]}, verbose=False)
        assert seen["prices_daily_usd"] == ["close_pr"]


class TestStartTunnel:
    def test_start_tunnel_injects_service_token_env(self, monkeypatch):
        captured = {}

        def fake_popen(cmd, stdout=None, stderr=None, env=None):
            captured["cmd"] = cmd
            captured["env"] = env
            return _FakeProc()

        monkeypatch.setattr(qd, "find_cloudflared", lambda: "cf.exe")
        monkeypatch.setattr(qd.subprocess, "Popen", fake_popen)
        monkeypatch.setattr(qd.time, "sleep", lambda s: None)

        db = _qdb(cf_client_id="cid", cf_client_secret="csec", tunnel_wait=0)
        proc = db._start_tunnel()

        assert proc.pid == 4321
        assert captured["cmd"] == ["cf.exe", "access", "tcp", "--hostname",
                                   "shquantdb.alphawaves.vip", "--url", "127.0.0.1:15432"]
        assert captured["env"]["TUNNEL_SERVICE_TOKEN_ID"] == "cid"
        assert captured["env"]["TUNNEL_SERVICE_TOKEN_SECRET"] == "csec"

    def test_start_tunnel_no_token_when_absent(self, monkeypatch):
        # _clean_env 픽스처가 TUNNEL_SERVICE_TOKEN_* 를 이미 제거 → 상속 env 에 없음
        captured = {}
        monkeypatch.setattr(qd, "find_cloudflared", lambda: "cf.exe")
        monkeypatch.setattr(qd.subprocess, "Popen",
                            lambda cmd, stdout=None, stderr=None, env=None: captured.update(env=env) or _FakeProc())
        monkeypatch.setattr(qd.time, "sleep", lambda s: None)

        db = _qdb(cf_client_id="", cf_client_secret="", tunnel_wait=0)
        db._start_tunnel()
        assert "TUNNEL_SERVICE_TOKEN_ID" not in captured["env"]
        assert "TUNNEL_SERVICE_TOKEN_SECRET" not in captured["env"]

    def test_explicit_cloudflared_bin_used(self, monkeypatch):
        captured = {}

        def _boom():
            raise AssertionError("find_cloudflared should not be called when cloudflared_bin set")

        monkeypatch.setattr(qd, "find_cloudflared", _boom)
        monkeypatch.setattr(qd.subprocess, "Popen",
                            lambda cmd, stdout=None, stderr=None, env=None: captured.update(cmd=cmd) or _FakeProc())
        monkeypatch.setattr(qd.time, "sleep", lambda s: None)

        db = _qdb(cloudflared_bin="/custom/cf.exe", tunnel_wait=0)
        db._start_tunnel()
        assert captured["cmd"][0] == "/custom/cf.exe"

    def test_falls_back_to_find_cloudflared(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(qd, "find_cloudflared", lambda: "/auto/cf.exe")
        monkeypatch.setattr(qd.subprocess, "Popen",
                            lambda cmd, stdout=None, stderr=None, env=None: captured.update(cmd=cmd) or _FakeProc())
        monkeypatch.setattr(qd.time, "sleep", lambda s: None)

        db = _qdb(tunnel_wait=0)  # cloudflared_bin 미지정 → 자동탐지
        db._start_tunnel()
        assert captured["cmd"][0] == "/auto/cf.exe"

    def test_start_tunnel_missing_cloudflared_raises(self, monkeypatch):
        monkeypatch.setattr(qd, "find_cloudflared", lambda: None)
        db = _qdb(tunnel_wait=0)
        with pytest.raises(RuntimeError):
            db._start_tunnel()


class TestContextManagerLifecycle:
    def test_enter_starts_tunnel_and_engine_exit_cleans_up(self, monkeypatch):
        events = []
        fake_engine = _FakeEngine(_FakeConn(_FakeResult([], [])))

        def fake_start(self):
            events.append("tunnel_start")
            self._tunnel = _FakeProc()
            return self._tunnel

        def fake_verified(self, dsn, **kw):
            events.append(("engine", dsn))
            return fake_engine

        def fake_kill(self):
            events.append("tunnel_kill")
            self._tunnel = None

        monkeypatch.setattr(QuantDB, "_start_tunnel", fake_start)
        monkeypatch.setattr(QuantDB, "_create_verified_engine", fake_verified)
        monkeypatch.setattr(QuantDB, "_kill_tunnel", fake_kill)

        with _qdb() as db:
            assert db.engine is fake_engine

        assert events[0] == "tunnel_start"
        assert events[1][0] == "engine"
        assert events[1][1].startswith("postgresql://u:p@127.0.0.1:15432/quantdb")  # +statement_timeout options
        assert "tunnel_kill" in events
        assert fake_engine.disposed is True

    def test_local_host_skips_tunnel_and_uses_5432(self, monkeypatch):
        events = []
        fake_engine = _FakeEngine(_FakeConn(_FakeResult([], [])))
        monkeypatch.setattr(QuantDB, "_start_tunnel", lambda self: events.append("tunnel_start") or _FakeProc())
        monkeypatch.setattr(QuantDB, "_create_verified_engine",
                            lambda self, dsn, **kw: events.append(("engine", dsn)) or fake_engine)
        monkeypatch.setattr(QuantDB, "_kill_tunnel", lambda self: events.append("tunnel_kill"))

        with _qdb(local_host=True) as db:
            assert db.engine is fake_engine

        assert "tunnel_start" not in events                          # 터널 안 띄움
        dsn = next(e[1] for e in events if isinstance(e, tuple))
        assert dsn.startswith("postgresql://u:p@127.0.0.1:5432/quantdb")  # 로컬 Postgres 직결 (포트 5432)
        assert fake_engine.disposed is True

    def test_exit_cleans_up_on_exception(self, monkeypatch):
        fake_engine = _FakeEngine(_FakeConn(_FakeResult([], [])))
        monkeypatch.setattr(QuantDB, "_start_tunnel", lambda self: setattr(self, "_tunnel", _FakeProc()) or self._tunnel)
        monkeypatch.setattr(QuantDB, "_create_verified_engine", lambda self, dsn, **kw: fake_engine)
        killed = []
        monkeypatch.setattr(QuantDB, "_kill_tunnel", lambda self: killed.append(True))

        with pytest.raises(ValueError):
            with _qdb():
                raise ValueError("boom")

        assert fake_engine.disposed is True
        assert killed == [True]

    def test_enter_failure_kills_tunnel(self, monkeypatch):
        # 엔진 생성이 __enter__ 에서 실패하면 __exit__ 가 안 돌므로 터널을 직접 정리해야 한다.
        killed = []
        monkeypatch.setattr(QuantDB, "_start_tunnel", lambda self: setattr(self, "_tunnel", _FakeProc()) or self._tunnel)
        monkeypatch.setattr(QuantDB, "_create_verified_engine",
                            lambda self, dsn, **kw: (_ for _ in ()).throw(RuntimeError("engine fail")))
        monkeypatch.setattr(QuantDB, "_kill_tunnel", lambda self: killed.append(True))

        with pytest.raises(RuntimeError):
            with _qdb():
                pass

        assert killed == [True]
