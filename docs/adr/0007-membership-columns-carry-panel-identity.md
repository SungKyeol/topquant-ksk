# membership 컬럼은 패널과 **같은 식별자 축**을 갖는다 (relations identity 합집합)

`fetch_etf_universe_panel` 의 membership 은 `MultiIndex(isin, tradingitemid)` 였다. 패널은 `MultiIndex(item, ticker, name, isin, tradingitemid)` 다. 두 산출물을 나란히 쓰는 코드가 매번 축을 손으로 맞춰야 했고, membership 만 보면 그 라인이 **무슨 종목인지 읽을 수가 없었다** — ISIN 과 정수 tid 뿐이라 눈으로 검증이 안 된다.

## Decision

membership 컬럼 레벨 = **요청한 relations 들의 identity 레벨 합집합**. 단 `isin` 과 `tradingitemid` 는 합집합에 없더라도 항상 남는다 — membership 자신의 키이고, 빼면 시대별로 갈린 라인 둘(ADR-0005 의 SLM `263819154`/`2644409`)이 같은 라벨이 되어 컬럼이 겹친다. 순서는 패널과 같은 캐논 순서 `(ticker, name, isin, tradingitemid)` 다.

| relations | 합집합 | membership 레벨 |
|---|---|---|
| `prices_daily_usd` | ticker, name, isin, tradingitemid | 그대로 4개 |
| `prices_daily_usd` + `spot_kr_daily` | ticker, name, isin, tradingitemid | 그대로 4개 |
| `spot_kr_daily` 단독 (tid 없음) | ticker, name, isin | + `tradingitemid`(키) = 4개 |

값은 **이미 받아 온 패널 컬럼에서** 끌어온다 — 조회가 늘지 않는다. 조회 우선순위는 `tradingitemid` 매핑이 먼저고 그 다음이 `isin` 매핑이다 (한 ISIN 이 라인 둘로 갈렸을 때 tid 만이 둘을 구분한다). 같은 브리지끼리는 먼저 온 relation 이 이긴다.

## Consequences

- **패널에 없는 라인은 ticker/name 이 NaN 이다.** 이건 결함이 아니라 신호다 — "편입인데 이 relation 이 커버하지 않는다" 가 컬럼에 그대로 보인다. SPY+QQQ / `start=2015-01-01` 실측 5건:
  - `2665340`(Altaba), `12465726`(TFCF), `24993638`(Viacom) — 패널에는 있고 **name 은 차지만 ticker 만 NaN**. 셋 다 2019 년에 사라진 종목이고 벤더가 `prices_daily_usd.ticker` 를 통째로 NULL 로 둔다(행 3.5k~5k, ticker non-null 0).
  - `643728096`(US1101221570), `674559857`(US6745991629) — 패널에 컬럼 자체가 없어 둘 다 NaN. ADR-0006 의 "membership 에만 있는 2개".
- **정렬 축이 바뀐다.** `sort_index(axis=1)` 이 이제 ticker 부터 정렬한다 (전에는 isin). NaN ticker 는 뒤로 간다.
- **`uncovered` 파생은 그대로다** — `isin` 레벨이 남아 있다.
- relations 를 여러 개 주면 통화 뷰 간 ticker/name 이 다를 수 있는데, 먼저 온 relation 의 값이 이긴다. 축이 relations 인자 순서에 의존한다는 뜻이다.
