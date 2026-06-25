# QuantDB.fetch_timeseries 는 connectorx 로 bulk read (sqlalchemy engine 우회, SQL 인라인); pivot 은 pandas

`QuantDB.fetch_timeseries` 는 대용량 패널(수천 종목 × 수년)을 다루는데, 측정 결과 **병목은 read 이지 pivot 이 아니었다**. 그래서 데이터 fetch 는 **connectorx**(`connectorx.read_sql(..., return_type="pandas")`, Rust 멀티코어)로 읽어 sqlalchemy+pandas read 대비 ~2.4x 빠르게 하고, **pivot 은 pandas `DataFrame.pivot`** 를 그대로 쓴다. connectorx 는 `self.engine`(verified sqlalchemy pool)을 쓰지 않고 `self._dsn` 으로 (이미 떠 있는) 터널에 자기 커넥션을 직접 열며, `:name` 바인드를 지원하지 않으므로 WHERE 값은 `_sql_lit`(작은따옴표 2배 escape)로 SQL 문자열에 인라인한다. 컬럼감지 같은 작은 쿼리는 기존 `read_sql`(sqlalchemy)을 그대로 쓴다.

## Considered Options

- **sqlalchemy + pandas read (`read_sql`)** — 기각. 일관적이고 `:name` 바인드로 injection-safe 하지만 대용량 read 가 single-thread + Python-row materialize 로 느림 (벤치 13.5M행: 67.7s).
- **polars `melt → pivot` (connectorx → polars, 식별자를 `\x1f` key 로 인코딩 후 pivot)** — **벤치로 기각**. wide(수만 열) 패널에서 polars pivot 이 pandas 보다 **~114x 느렸다** (13.5M행: polars 773s vs pandas 6.8s; melt 가 13.5M→95M행으로 부풀고 polars pivot 이 고-cardinality `on` 에 취약). 전체로는 connectorx+polars 가 옛 코드보다 **10.8x 더 느림**(801s vs 74s). → polars pivot 폐기, pandas pivot 복원.
- **polars over sqlalchemy engine (`pl.read_database(connection=engine)`)** — 기각. connectorx 가 아니라 read 이득이 작음.

벤치(stock_kr_daily, 전체 종목, 2000~, 13.5M행) TOTAL: 옛 sqlalchemy+pandas 74.5s / connectorx+pandas **39.7s (1.9x)**.

## Consequences

- fetch_timeseries 의 **데이터 read 는 `self.engine` 을 쓰지 않는다** (컬럼감지 read_sql 만 engine 사용). connectorx 가 `pool_pre_ping`/verified-retry 없이 자기 커넥션을 연다 — 터널은 `with QuantDB(...)` 컨텍스트 안에서 이미 떠 있어야 동작.
- WHERE 값이 **bind 파라미터가 아니라 인라인**이다. `_sql_lit` 가 작은따옴표를 escape 해 injection 을 막지만 read_sql 의 `:name` 바인딩과 달라 보인다 — **connectorx 제약에 따른 의도된 설계**. "버그"로 보고 read_sql 로 되돌리지 말 것.
- pivot 은 pandas. polars 를 reshape 에 쓰지 말 것 (위 벤치 참조) — wide 패널에서 재앙적으로 느림.
- `connectorx` 가 `[db]` 의 사실상 필수 의존성이 된다. (`polars` 는 legacy download/upload 가 쓰며, fetch_timeseries 는 더 이상 쓰지 않는다.)
