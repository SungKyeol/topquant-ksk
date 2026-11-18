# item_name/item_value long 패널은 item 레벨에 item_name 값을 올린다 (PANEL_SHAPE_VER 3)

`fetch_timeseries` 는 relation 의 컬럼 이름과 타입만 보고 시간·식별자·값 역할을 정한다. 값 컬럼이 곧
columns 의 `item` 레벨이라 `panel["adj_close_pr"]` 처럼 필드로 꺼낸다.

`ai_ready.macro_daily` 는 모양이 다르다. 필드가 컬럼이 아니라 **행**이다 —
`(source, ticker, name, item_name, date, item_value)`. 2026-11-18 블룸버그 arm(quant-db ADR-0065)이
`VAR_SWAP_1M_LV` 와 `PX_LAST` 를 같은 뷰에 넣으면서 두 결함이 드러났다.

- `item_name` 이 문자열이라 값도 식별자도 아니어서 **SELECT 되지 않았다.** 결과 columns 가
  `("item_value", "SPX Index", "S&P 500 INDEX")` 라 그 값이 분산스왑인지 종가인지 알 수 없었다.
- 한 티커가 필드 둘을 가지면 `(date, ticker, name)` 이 겹쳐 pivot 이 `duplicate entries` 로 죽는다.

## 결정

relation 에 `item_name` 과 `item_value` 가 **둘 다** 있으면:

- `item_name` 을 피벗축 맨 앞에 넣고 `item_value` 를 값으로 쓴다 → columns
  `("VAR_SWAP_1M_LV", "SPX Index", "S&P 500 INDEX")`. 다른 패널과 같은 `panel["<필드>"]` 계약이다.
- `fields=` 는 그 relation 에서 **item_name 값**이다 → `WHERE item_name IN (...)`.
- columns 단수는 그대로(`item, ticker, name`)지만 item 레벨의 내용이 바뀌므로 `PANEL_SHAPE_VER` 를 3 으로 올린다.

판정은 컬럼 이름 쌍이다. 2026-11-18 pg_catalog 실측으로 `ai_ready` 에서 `item_name`·`item_value` 를
가진 객체는 `macro_daily` 하나뿐이라, 나머지 패널의 결과는 바뀌지 않는다(캐시 키만 하루 무효화).

## Considered Options

- **`pivot_by=` 옵트인 인자** — 기각. 기본 호출이 계속 필드를 잃고 필드가 둘이 되면 죽는다. 고치는 법을
  아는 사람만 피한다.
- **`item_name` 을 identity 에 추가** — 기각. columns 가 `("item_value", ticker, name, item_name)` 이 되어
  item 레벨이 늘 의미 없는 상수로 남고, 다른 패널과 꺼내는 법이 달라진다.
- **relation 이름으로 분기(`if relname == "macro_daily"`)** — 기각. 같은 모양의 뷰가 생기면 또 고쳐야 한다.

## Consequences

- `macro_daily` 를 `panel["item_value"]` 로 읽던 코드는 깨진다. 2026-11-18 에 `topquant-ksk`,
  `topquant_ksk_strategy`, `TopQuant`, `quant-db` 에서 그런 호출은 찾지 못했다.
- `source` 는 여전히 피벗축이 아니다. 두 벤더가 같은 `(ticker, item_name)` 을 쓰게 되면 pivot 이 다시
  `duplicate entries` 로 죽는다 — 그때는 `source` 를 identity 에 넣는 결정을 따로 한다.
