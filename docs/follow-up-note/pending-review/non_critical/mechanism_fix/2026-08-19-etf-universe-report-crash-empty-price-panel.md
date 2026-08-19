# report() 가 빈 가격 패널에서 죽고, 그 바람에 save() 가 아예 안 돈다

**Date raised**: 2026-08-19
**Source**: `hardcoded_credential/fetch_etf_universe_panel.py` 를 SPY 단일 → `FUNDS=("SPY","QQQ")` 합집합으로 바꾸는 작업 중 발견 (그 폴더는 `.gitignore` 되어 있어 파일 자체는 git 에 없다)
**Status**: Deferred

## Motivation

`report()` 는 가격 패널이 비어 있을 때 두 가지로 잘못 동작한다. 둘 다 실측했다.

1. **죽는다.** 요약의 마지막 줄이 `f"{px.index.min():%Y-%m-%d} ~ {px.index.max():%Y-%m-%d}"` 인데,
   `px` 가 0행이면 `px.index.min()` 은 `NaT` 이고 `f"{pd.NaT:%Y-%m-%d}"` 는
   `ValueError: NaTType does not support strftime` 로 터진다.

2. **경고가 조용히 사라진다.** 바로 아래 CIQ 덤프 지연 경고는
   `lag = con["month_end"].max() - px.index.max()` → `NaT`, `lag.days` → `nan` 이 되고
   `nan > 40` 은 False 다. 즉 **"가격이 한 줄도 없다" 는 가장 심한 경우에 경고가 안 뜬다.**
   경고가 존재하는 목적 그 자체를 놓치는 구간이다.

진짜 비용은 1번의 crash 자체가 아니라 **호출 순서**다. `main()` 이

```python
out = fetch(db)   # ← 수십 분짜리 DB pull
report(out)       # ← 여기서 죽으면
save(out)         # ← 여기까지 절대 못 온다
```

이므로, 요약 출력 한 줄 때문에 방금 받아온 패널 전체가 parquet 로 앉지 못하고 통째로 날아간다.
2026-08-19 에 이 스크립트의 전체이력 실행 2개가 87분을 돌고도 `out/` 을 남기지 못한 채 끝났다
(그 원인이 이것이라고 확인하지는 못했다 — 다만 순서가 그런 결말을 허용한다는 것은 확실하다).

빈 가격 패널은 가상의 시나리오가 아니다. CIQ 덤프 tail 은 홀딩스 tail 보다 뒤처지고 (실측
2026-08-19 기준 가격 2026-05-19 vs 홀딩스 07-31), `START` 를 그 tail 이후로 잡거나 CIQ 에 매핑되는
`tradingitemid` 가 하나도 없는 유니버스를 고르면 바로 밟는다.

관련(별건): `pending-review/2026-06-25-fetch-timeseries-empty-result-shape` — 그쪽은 라이브러리
`QuantDB.fetch_timeseries` 의 빈 결과 *모양* 문제이고, 이 note 는 소비 스크립트가 빈 결과에 죽는 문제다.

## Proposed approach

1. `report()` 에서 `px.empty` 를 먼저 갈라낸다 — 가격 패널 줄과 lag 경고를 "가격 0행 (CIQ 매핑
   0건이거나 기간이 덤프 tail 이후)" 한 줄로 대체하고, 구성종목·GICS·매핑 요약은 그대로 계속 낸다.
   `lag` 계산도 `px.index.max()` 가 `NaT` 이면 건너뛴다.
2. `main()` 에서 `save(out)` 를 `report(out)` **앞으로** 옮긴다. 비싼 산출물을 먼저 디스크에
   앉히고 그 다음에 사람이 읽을 요약을 찍는 순서가 맞다 — 1번을 하더라도 report 의 다른 줄이
   미래에 또 죽을 수 있고, 그때도 데이터는 살아남아야 한다.

두 개는 독립이고 2번만으로도 데이터 유실은 막힌다.

## Non-goals

- 빈 가격 패널을 에러로 승격시키는 것. 지금 필요한 것은 **죽지 않고 알리는 것**까지다.
- REPL 대응 (`__file__` 가드, `raise SystemExit` → `ValueError`). 같은 파일의 별건이고 이미 사용자에게
  패치안을 제시해 둔 상태다.
