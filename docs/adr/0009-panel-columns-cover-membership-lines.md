# 패널 컬럼은 membership 라인 축을 **포함**한다 + isin 라벨은 tid 의 최신 ISIN 하나로 통일

`fetch_etf_universe_panel` 의 두 산출물은 축이 달랐다. SPY+QQQ 전 이력에서 패널 1,224 tid / membership 1,227 라인이었고, 그 차이가 어디서 오는지는 컬럼만 봐서는 알 수 없었다. 편입인데 가격이 없는 라인은 패널에 **컬럼조차 없어서**, `panel * membership` 이나 `reindex` 가 그 종목을 조용히 잃었다.

## Decision

**(1) 패널은 항상 membership 라인 축을 포함한다.** 가격이 없는 편입 라인도 전부-NaN 컬럼으로 들어간다 (실측 6개: 미커버 5 + isin 키 1). 자리가 있어야 곱셈·정렬이 라인을 잃지 않는다.

**(2) `isin` 라벨은 그 tid 의 최신 ISIN 하나로 통일한다.** 패널과 membership 양쪽을 같은 map 으로 덮는다:

```sql
SELECT DISTINCT ON (v.tradingitemid) v.tradingitemid, v.isin
FROM ai_ready.isin_tradingitem v WHERE v.tradingitemid = ANY(:t)
ORDER BY v.tradingitemid, (v.via = 'isin_span') DESC,
         (upper(v.span) IS NULL) DESC, lower(v.span) DESC NULLS LAST
```

안 하면 20개 라인이 tid 는 같은데 라벨이 달라 안 붙는다 — 패널은 가격 행에 **현재 기준으로 각인된** ISIN(ADR-0004)을, membership 은 홀딩스가 **그때 적은** ISIN 을 들기 때문이다. 실측: ACKH, ASN, ASO, BATR.A/K, BUD, CBSS, CGP, CHA, FBF, GBLX.Q, NXTL, SOV, SSCC.Q, SUB, TWX, USW, WB, WM. ADR-0008 이 "마지막 편입 시점 ISIN 이 곧 각인된 ISIN" 이라고 본 것은 VMED 한 건에서만 맞았다.

**(3) `drop_unheld_panel_columns=True`(기본 False)** 면 membership 에 없는 패널 컬럼을 버려 두 축이 **정확히** 같아진다.

| | 패널 라인 | membership | 차집합 |
|---|---|---|---|
| 기본 (무손실) | **1,230** | 1,227 | 3 (CXT·Q·VMRK) |
| `drop_unheld_panel_columns=True` | **1,227** | 1,227 | **0** |

버려지는 3개는 정상이다. membership 은 `h.month_end <@ v.span` 으로 만들어지므로, **membership 에 없는 tid 는 정의상 편입된 달을 자기 span 에 담고 있지 않다**(대우). 실측으로도 셋 다 홀딩스 기간과 겹침 0개월이다.

## VMRK — "버리면 구멍" 판정을 값 유무로 하면 안 되는 이유

`US29476L1070` 은 라인이 둘이다: `2610085`(EQR, 2000-01-03~**2026-08-17**, 6,695행)와 `2016912246`(VMRK, **2026-05-20**~2026-08-24, 65행). span 경계는 2026-08-18 인데 **가격은 5/20 부터 두 라인에 겹쳐 찍힌다** — ADR-0005 가 "오귀속 60행" 이라 부른 그것이다.

VMRK 컬럼에는 편입 구간(홀딩스 ~2026-07-31) 안의 값이 분명히 있다. 그래서 "편입 구간에 값이 있으면 버리지 마라" 로 판정하면 경고가 뜬다. **그러나 그 날짜는 EQR 컬럼이 이미 덮는다** — 버려도 구멍이 안 난다. 그래서 판정은 값 유무가 아니라 **"그 날짜에 같은 ISIN 의 membership 라인 컬럼도 전부 NaN 인가"** 여야 한다. 실측 이 기준으로 SPY+QQQ 전 이력의 구멍은 0건이다.

잃는 것은 8/18~8/24 5행뿐이고, 그 구간은 마지막 month_end(7/31) 이후라 membership 이 아직 판단할 근거가 없다. 8/31 홀딩스가 들어오면 span 이 VMRK 를 지목해 컬럼이 생긴다.

## Considered Options

- **호출자가 직접 reindex** — 기각. `panel.reindex(columns=membership.columns)` 는 **조용히 틀린다**: identity 에 NaN 이 있으면(상장폐지로 벤더가 ticker 를 비운 종목 20건) NaN != NaN 이라 멀쩡한 컬럼이 전부-NaN 으로 바뀐다. 라이브러리가 정규화 키로 위치를 찾아 재배치해야 한다.
- **기본값을 drop 으로** — 기각. 무손실이 기본이어야 한다. 버리는 것은 옵트인.
- **membership 을 패널 축에 맞추기(반대 방향)** — 기각. 가격이 없는 편입 라인이 사라져 생존편향이 생긴다.
- **isin 라벨을 패널 각인값으로** — 기각. 패널에 컬럼이 없는 라인은 각인값이 없어 규칙이 반쪽이 된다. 브리지의 최신 ISIN 은 양쪽 모두에 있다.

## Consequences

- **패널의 `isin` 레벨이 원본 뷰의 `isin` 과 다를 수 있다** (재발급 종목 20건). tradingitemid 가 정체성이고 isin 은 라벨이다.
- **`uncovered` 는 컬럼 존재로 판정하면 안 된다** — 이제 편입 라인은 가격이 없어도 컬럼이 있다. `panel.columns[panel.notna().any()]` 로 값 유무를 봐야 한다.
- **식별자 축이 다른 relation 은 정렬하지 않는다.** `spot_kr_daily` 처럼 tradingitemid 가 없는 패널은 membership 라인 여럿이 같은 라벨로 뭉개져 1:1 이 성립하지 않는다 — 경고하고 원본을 그대로 돌려준다.
- **tid 레벨은 정렬 후에도 nullable `Int64`** 다. 튜플로 MultiIndex 를 만들면 `<NA>` 때문에 float64 로 추론돼 100 이 100.0 이 되고 ADR-0008 이 잡아 둔 것이 도로 깨진다 — `_line_index` 가 한 곳에서 dtype 을 고정한다.
- **캐시 무효화**: `UNIVERSE_SHAPE_VER` 3 → 4.
