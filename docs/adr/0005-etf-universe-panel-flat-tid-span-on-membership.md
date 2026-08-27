# ETF 유니버스 → 가격: 가격은 tradingitemid **평면** 조회, span 은 **membership 에만**

`QuantDB.fetch_etf_universe_panel` 은 ETF 구성종목을 유니버스로 잡아 가격 패널과 편입행렬을 함께 돌려준다. 두 산출물이 **정체성을 다르게 해석한다** — 의도된 비대칭이다.

- **가격**: 유니버스 ISIN 을 `ai_ready.isin_tradingitem` 으로 tradingitemid 목록으로 번역한 뒤, `fetch_timeseries(..., filter_by="tradingitemid")` 로 **날짜 조건 없이 평면 조회**한다.
- **membership**: 같은 뷰를 `month_end <@ v.span` 으로 **조인**해 각 (ISIN, 월) 의 시점 정확한 라인을 찾는다.

즉 span(시점 인지 매핑)은 홀딩스 쪽에만 적용하고 가격 쪽에는 적용하지 않는다.

## 왜 — "당연해 보이는" span 조인이 데이터를 잃는다

SPY+QQQ 전 이력 유니버스(1,227 ISIN)로 세 경로를 실측했다 (2026-08-26/27):

| 경로 | 행 | ISIN | 소요 | `fetch_timeseries` 로 표현 가능? |
|---|---|---|---|---|
| A. `prices.isin = holdings.holding_isin` 직결 | 5,444,817 | 1,201 | 33.8s | ✅ `filter_by="isin"` |
| B. `isin_tradingitem` **조인** (`date <@ span`) | 5,478,015 | 1,222 | 35.4s | ❌ 조인은 불가 |
| **C. tid 목록으로 평면 조회** | **5,476,029** | **1,221** | **16.3s** | ✅ `filter_by="tradingitemid"` |

C 가 B 의 **99.96%** 를 절반 시간에 얻는다(차이 1,986행, -0.04%). A 대비로는 ISIN 을 20개 더 건진다 — `prices_daily_usd.isin` 은 가격 행에 **현재 기준으로 각인된** ISIN 이라, ISIN 이 재발급된 종목은 홀딩스가 든 ISIN 과 어긋나 직결이 실패한다. tid 로 내려갔다 오면 그 어긋남을 통과한다.

B 가 국지적으로 하는 일은 양면이다. `US29476L1070` 의 오귀속 VMRK 60행(2026-05-20~08-17)을 정확히 걸러내지만, `US78442P1066`(SLM) 에서는 **636행을 잃는다**(2000-01-03~2002-07-17). 그 구간을 `isin_tradingitem` 의 시점무시 폴백 등급(`via='isin'`)이 `263819154`(SLM Corporation)로 지목하는데, 실제 그 시기 가격은 `2644409`(Navient) 밑에 있어서 조인 조건이 어긋나 탈락한다. 폴백 등급이 있는 한 span 조인은 **가격을 조용히 지운다**.

반면 membership 쪽에서는 span 이 싸고 정확하다 — 홀딩스는 190,642행뿐이라 **4.1초**이고, 해석 실패는 119행(0.06%, 고유 ISIN 1개)이다. 그리고 SLM 이 실제로 시점별로 갈린다: `263819154` 는 1999-12~2002-06(31개월), `2644409` 는 2002-07~2014-04(142개월). span 해석 tid 집합(1,226)은 평면 브리지 tid 집합(1,230)의 **부분집합**이다. ~~그래서 membership 컬럼마다 대응하는 패널 컬럼이 반드시 있다.~~ **정정(ADR-0006)**: 부분집합인 대상은 *패널에 넣은 브리지 tid 목록* 이지 *패널 컬럼* 이 아니다 — 패널은 실제 가격 행이 있는 tid 만 컬럼으로 남기므로 양방향으로 어긋난다.

## Considered Options

- **경로 B 를 그대로 쓰기** — 기각. 위 636행 손실 + 2배 소요. 무엇보다 `date <@ span` 은 **조인**이라 `fetch_timeseries`(필터+피벗) 로 표현할 수 없어, 유니버스 경로만 `read_sql` 로 갈라진다.
- **경로 A (ISIN 직결)** — 기각. 홉은 하나 적지만 ISIN 재발급 때문에 20종목을 놓친다(1,201 vs 1,221).
- **membership 을 원본 축(월 × ISIN)으로 두기** — 기각. 패널은 tid 로 키가 잡히는데 membership 이 ISIN 이면 두 산출물을 잇는 매핑이 따로 필요해진다. tid 축으로 맞추면 그 매핑이 컬럼 자체에 들어간다.
- **membership 을 tid **단독**으로 키잉** — 기각. 패널 정렬은 되지만 ISIN 결을 잃어 미커버 목록이 안 나온다. `(isin, tradingitemid)` MultiIndex 가 둘 다 만족한다. (**정정(ADR-0008)**: 키는 tid 단독으로 좁혔다 — 기각 사유는 키와 레벨을 같은 것으로 본 데서 나왔고, isin 은 레벨로 남아 미커버 파생이 그대로다. **확장(ADR-0007)**: 그 둘을 키로 유지한 채 relations 의 identity 레벨 합집합 — 보통 `ticker`/`name` — 까지 컬럼에 싣는다.)
- **`universe`(isin↔tid 매핑 + 커버 여부) 를 세 번째 반환값으로** — 기각. 파생 가능해서 잉여다: 매핑은 `panel.columns` 가 `isin` 레벨을 들고 있어 바로 나오고(ADR-0004 의 identity 확장 덕분), 미커버는 `set(membership.columns.get_level_values("isin")) - set(panel.columns.get_level_values("isin"))` 이다. 게다가 relation 마다 커버리지가 달라(tid 1,221 vs isin 1,201) 표 하나로는 표현이 **틀린다**.
- **membership 을 패널 날짜축에 정렬해서 반환** — 기각. 정렬하려면 선견편향 결정을 내려야 하는데 데이터가 답을 주지 않는다. `as_of` 가 `month_end` 와 **항상 같아서**(190,642행 전부, 320개월) 공시 지연을 알 수 없고, `month_end` 는 거래일이 아닐 때가 많다(2024-01 이후 31개 중 25개만 거래일). 라이브러리가 조용히 고르면 사용자가 몇 개월치 선견편향을 모른 채 쓰게 된다. 정렬은 호출자 몫으로 남기고 예제에 `shift(1)` 과 경고를 적어 둔다.

## Consequences

- **가격은 시점 정확하지 않다.** ISIN 을 나눠 갖는 라인들이 전부 딸려 오므로, 오귀속 구간(EQR/VMRK 60일 같은)이 패널에 남는다. 시점 정확한 가격이 필요하면 `read_sql` 로 `date <@ span` 조인을 직접 쓰되 위 636행류 손실을 감수해야 한다.
- **membership 은 시점 정확하지만 폴백 구간이 있다.** `via='isin'` 으로 해석된 행(SPY+QQQ 실측 21,407행)은 편입 True 인데 패널이 NaN 일 수 있다 — 경고로 알린다.
- **relation 마다 브리지 컬럼이 다르다.** `tradingitemid` 가 있으면 그것, 없으면 `isin`, 둘 다 없으면 `ValueError`. 그래서 relation 에 따라 커버리지가 달라진다(1,221 vs 1,201). 뷰 목록을 하드코딩하지 않으므로 새 통화 뷰가 생겨도 자동으로 받아들인다.
- **`membership.index` 는 `to_datetime` 된다.** 패널 index 와 타입이 같아야 정렬이 되기 때문이다.
- 반환은 `EtfUniversePanel(panels: dict, membership: DataFrame)` 이고 `panels` 는 relation 이 하나여도 **항상 dict** 다 — 인자 개수에 따라 반환 타입이 바뀌면 호출자가 매번 분기해야 한다.
