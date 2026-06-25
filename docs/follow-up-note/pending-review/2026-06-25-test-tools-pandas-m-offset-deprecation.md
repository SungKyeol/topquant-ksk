# tools.py / test_tools.py 가 pandas 2.2+ 'M' offset deprecation 으로 깨짐

**Date raised**: 2026-06-25
**Source**: QuantDB.fetch_timeseries pickle 캐시 구현 중 전체 테스트 실행에서 발견 (`tests/test_tools.py` 6 failed)
**Status**: Deferred

## Motivation

pandas 2.2 부터 offset alias `'M'`(월말)이 deprecated → `'ME'`. 현재 Anaconda pandas 에서 `pd.date_range(..., freq="M")` 가 다음으로 raise:

```
ValueError: Invalid frequency: M. Failed to parse with error message:
ValueError("'M' is no longer supported for offsets. Please use 'ME' instead.")
```

영향 위치:

- `src/topquant_ksk/tools.py:27` — `def resample_last_date(data_daily, freq='M')` 의 기본값 `'M'`
- `tests/test_tools.py` — `pd.date_range(..., freq="M")` 8곳(55, 62, 70, 146, 168, 175 …) + `resample_last_date(data, freq="M")` 2곳(131, 139)

→ `TestAnnualizedTurnover` 3개 + `TestRoundingTargetWeight` 3개 = **6개 테스트 ValueError 로 실패**.

이 작업(quantdb pickle 캐시)과 무관 — `git diff --name-only` 는 `quantdb.py` / `test_quantdb.py` 만. 기존 코드의 pandas 버전 비호환.

## Proposed approach

`'M'` → `'ME'` 교체:

- `tools.py` `resample_last_date` 기본값 `freq='ME'`.
- `test_tools.py` 의 `freq="M"` → `"ME"`.
- 하위호환을 원하면 `resample_last_date` 내부에서 `'M'→'ME'` 정규화 후 `DeprecationWarning` 한 번.
- `risk_return_metrics.py` 의 `'W-FRI'` 는 여전히 유효 → 손대지 않음. `'Q'`/`'Y'` 사용처가 추가로 있으면 `'QE'`/`'YE'` 도 같이.

## Non-goals

- pandas 버전 핀다운/다운그레이드 — 코드 수정이 정답.
- quantdb 경로 변경 — 무관.
