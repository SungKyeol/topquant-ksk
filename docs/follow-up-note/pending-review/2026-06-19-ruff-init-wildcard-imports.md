# src/topquant_ksk/__init__.py 의 ruff F403/F401 → CI lint 실패

**Date raised**: 2026-06-19
**Source**: QuantDB 작업 중 `ruff check src/` (CI `ci.yml` lint step) 실행에서 발견
**Status**: Deferred

## Motivation

CI 의 lint 단계(`ruff check src/`)가 `src/topquant_ksk/__init__.py` 때문에 exit 1 로 실패한다:

- `F403` — `from .risk_return_metrics import *` / `.load_data` / `.plot` / `.tools` (wildcard import ×4)
- `F401` — `from . import db` (try/except 안의 unused import)

이는 **pre-existing** 이다 (이번 브랜치가 만든 게 아니라 `main` 에서도 이미 red). 이번 작업의 신규 코드(`db/quantdb.py` 등)는 `ruff check` clean 이다. 다만 lint step 이 red 인 채로 두면 PR 체크가 계속 실패한다.

## Proposed approach

`src/topquant_ksk/__init__.py` 만 손보면 된다 (다른 파일 영향 없음):

- wildcard import 4개: `__all__` 를 각 서브모듈에 정의해 명시적 re-export 로 바꾸거나, 의도된 패턴이면 파일 상단에 `# ruff: noqa: F403` (또는 라인별 `# noqa: F403`).
- `from . import db`: optional 서브패키지 존재 확인이 목적이므로 `# noqa: F401` 또는 `importlib.util.find_spec` 패턴으로 교체.

## Non-goals

- QuantDB 기능과 무관. legacy `__init__.py` 정리라 이번 범위 밖으로 분리.
