# fetch_timeseries 가 0행일 때 flat empty df 반환 (MultiIndex 불일치)

**Date raised**: 2026-06-25
**Source**: QuantDB.fetch_timeseries 구현/검증 중 발견 (commit 4e58685)
**Status**: Deferred

## Motivation

`fetch_timeseries` 는 정상 결과는 `columns=MultiIndex(item, ticker, name, isin)` 의 wide DataFrame 을 반환한다. 그런데 조건에 맞는 행이 0개면 (예: 없는 ticker, 데이터 없는 기간) `if long.empty: return long` 로 **pivot 하지 않은 flat empty DataFrame** 을 반환한다.

결과적으로 호출측이 빈 결과에 `panel['close']` 또는 `panel.columns.get_level_values('item')` 를 하면 `KeyError` 가 난다 — non-empty 와 구조가 달라서 surprising 하다. 검증 중 실제로 이 KeyError 를 두 번 밟았다.

단, 이건 **cosmetic** 에 가깝다: 0행이면 엔티티가 없어 컬럼 자체가 0개이므로, MultiIndex 로 만들어도 `panel['close']` 는 어차피 (컬럼이 없어) 실패한다. 즉 빈 결과는 본질적으로 호출측이 `.empty` 를 먼저 확인해야 한다.

## Proposed approach

둘 중 하나:

1. **문서화로 충분** (최소): docstring 에 "0행이면 빈 DataFrame 반환 — 사용 전 `.empty` 확인" 명시. verbose 메시지(`0행`)는 이미 있음.
2. **구조 일관성** (선택): empty 일 때도 `index.name=time_col` + `columns=MultiIndex.from_arrays([[]]*(1+len(identity)), names=["item"]+identity)` 로 빈 MultiIndex 반환. `.columns.names` 는 일치하지만 컬럼 접근은 여전히 불가(데이터 없음) → 가치 낮음.

추천: 1번(문서화). 2번은 효용 대비 코드 추가가 아깝다.

## Non-goals

- 빈 결과에서 `panel['close']` 가 동작하게 만드는 것 — 데이터가 없으면 컬럼도 없으므로 불가능. 호출측 `.empty` 체크가 정답.
