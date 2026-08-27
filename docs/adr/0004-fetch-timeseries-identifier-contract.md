# fetch_timeseries 의 식별자 계약: `ids` + `filter_by`, identity 에 tradingitemid (0.2.0, 깨는 변경)

`QuantDB.fetch_timeseries` 가 "무엇으로 거르고 무엇으로 피벗하는가"를 0.2.0 에서 바꿨다. 세 조각이고 셋 다 하위호환을 깬다.

1. **`tickers=` → `ids=` 하드 개명** (별칭 없음). 이 인자는 이미 오래전부터 ticker 만 받지 않았다 — `fx_daily` 는 통화코드(`KRW`)를 받는다. 여기에 ISIN 과 `tradingitemid` 가 더해지면서 이름이 내용을 완전히 잘못 설명하게 됐다.
2. **`filter_by=` 추가 (키워드 전용)**. `ids` 를 어느 컬럼에 걸지 고른다. `None` 이면 기존 동작(그 relation 의 첫 식별자 = `ticker`, 없으면 `iso_code`). identity 에 없는 컬럼도 지정할 수 있다 — WHERE 절에만 쓰이기 때문이다.
3. **identity 에 `tradingitemid` 추가 (맨 뒤)**. 그 컬럼을 가진 패널(`prices_daily_krw` / `_local` / `_usd`, 3개뿐)의 columns MultiIndex 가 `(item, ticker, name, isin)` → `(item, ticker, name, isin, tradingitemid)` 로 한 단 늘어난다.

## 왜 필요했나 — 조용한 오염과 우연한 유일성

**필터**: `prices_daily_*` 는 여러 나라의 상장을 한 패널에 담는다. 티커 문자열은 나라마다 재사용되므로 ticker 로 거르면 같은 문자열의 외국 종목이 **에러 없이** 섞인다. SPY+QQQ 전 이력 유니버스로 실측(2026-08-26): ISIN 으로 걸면 5,444,817행 / 1,201 종목, 같은 유니버스에서 파생한 ticker 로 걸면 6,232,590행 / 1,422 종목 — **+787,773행(+14.5%), +221 종목**. `CA`→Carrefour SA, `DG`→Vinci SA, `EL`→EssilorLuxottica, `ASML`→ASML Holding N.V. 등 전부 EUR 상장이다. 피벗이 깨지지도 않으므로 아무 신호가 없다.

**피벗축**: `(date, tradingitemid)` 는 중복이 없지만 `(date, ticker, name, isin)` 은 보장이 없다. 한 ISIN 을 두 라인이 나눠 갖고 **날짜까지 겹치는** 경우가 실재한다 — `US29476L1070` 을 `EQR "Equity Residential"`(2610085) 과 `VMRK "Vivmark Residential"`(2016912246) 이 60일간 공유한다. 지금 피벗이 안 깨지는 유일한 이유는 그 두 라인의 `name` 이 다르다는 것뿐이다. 이름까지 같았으면 `ValueError: Index contains duplicate entries` 였다. `tradingitemid` 를 identity 에 넣으면 이 유일성이 **우연에서 구조적 보장으로** 바뀐다.

## Considered Options

- **`filter_by` 기본값을 `"isin"` 으로** — 기각. `isin` 컬럼을 가진 뷰가 13개인데 거기엔 `spot_kr_daily` / `spot_kr_5min` / `spot_kr_monthly` / `etf_kr_daily` / `etf_kr_5min` 이 전부 포함된다. 기본을 isin 으로 두면 `fetch_timeseries("spot_kr_5min", ids="A027970")` 가 `WHERE isin IN ('A027970')` → **에러 없이 0행**이 된다. 한국 주식 계열 전체가 조용히 죽는다. 게다가 `index_daily` 처럼 isin 이 없는 뷰는 ticker 로 떨어져, 같은 인자가 뷰에 따라 다른 컬럼을 가리키게 된다.
- **`tickers=` 를 `DeprecationWarning` 별칭으로 한 릴리스 유지** — 기각(사용자 결정). 레포 안 호출자가 34곳뿐이고 그중 비-테스트는 `hardcoded_credential/` 의 예제 2개와 `build_guide.py` 뿐이다. `docs/*.md` 와 `.claude` 스킬에는 언급이 0건이다. 한 번에 끊는 편이 두 이름이 공존하는 기간보다 낫다고 판단했다. (`example_db_usage.py` 등이 쓰는 `conn.download.fetch_timeseries_table` 은 **다른 레거시 API** 라 영향 없음.)
- **`tickers` 가 dict 도 받게 (`tickers={"isin": [...]}`)** — 기각. 한 인자가 list/dict 두 타입을 받으면 시그니처가 흐려진다.
- **`where=` 자유 조건 문자열** — 기각. connectorx 제약으로 WHERE 는 인라인 SQL 이라(ADR-0003) injection 면을 여는 것과 같다.
- **`identity=` 를 호출자가 지정** — 기각. 같은 뷰라도 호출 인자에 따라 반환 컬럼 모양이 달라져 캐시·다운스트림이 조용히 어긋난다. 지금 필요한 자유도도 아니다.
- **`prices_daily_*` 의 기본 엔티티만 `tradingitemid` 로** — 기각. `fx_daily` 가 `iso_code` 인 것과 같은 결이라는 주장이 있었지만, `fx_daily` 는 `ticker` 컬럼이 **아예 없어서** 강제된 폴백이고 `prices_daily_*` 는 멀쩡히 동작하는 컬럼을 덮는 것이다. 대신 **ticker 로 거르면 경고**를 띄운다(위 오염 수치 근거).

## Consequences

- **`tradingitemid` 는 identity 리스트의 맨 뒤여야 한다.** `entity = identity[0]` 이 기본 필터 컬럼이므로 앞에 두면 기존 `ids=["AAPL"]` 호출이 tid 로 걸려 조용히 0행이 된다. 순서가 계약의 일부다.
- **`_cache_sig` 에 `filter_by` 와 `shape_ver` 가 들어간다.** filter_by 가 키에 없으면 `ids=["US0378331005"]` 를 ticker 로 건 호출과 isin 으로 건 호출이 **같은 pkl** 을 가리켜 앞의 결과가 뒤에 조용히 돌아온다. `shape_ver`(현재 2)는 업그레이드 당일 남아 있던 4단 pkl 이 5단 코드에 히트하는 것을 막는다. columns 단수가 또 바뀌면 올릴 것.
- **`filter_by` 는 키워드 전용(`*`)이다.** `end` 뒤 위치인자로 두면 `save_and_reload_pickle_cache` 가 한 칸 밀려, 6번째 위치로 캐시 플래그를 넘기던 코드가 말없이 `filter_by=True` 로 해석된다.
- **`filter_by` 는 WHERE 만 바꾸고 피벗축은 건드리지 않는다.** tid 로 걸어도 컬럼은 여전히 identity 전체로 만들어진다.
- **한국 계열 패널은 아무 영향이 없다** — `spot_kr_*` / `etf_kr_*` / `index_daily` / `fx_daily` 에는 `tradingitemid` 컬럼이 없어 4단(또는 3단) 그대로다.
- 한 ISIN 이 둘 이상의 `(ticker, name)` 으로 갈리면 **경고**한다. 피벗이 깨지지 않는다는 것이 곧 조용하다는 뜻이라, 한 종목이 컬럼 둘로 나뉜 채 각 구간 밖이 NaN 이 되는 것을 쓰는 쪽이 모른다.
