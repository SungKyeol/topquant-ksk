# upload.py 4개 uploader 의 DB URI 에 quote_plus 누락

**Date raised**: 2026-06-18
**Source**: grill-with-docs 세션 (QuantDB 설계) 중 발견 — `src/topquant_ksk/db/upload.py:318,498,695,1056`
**Status**: Deferred

## Motivation

`upload.py` 의 4개 업로더가 DB 접속 URI 를 만들 때 비밀번호에 `quote_plus` 를 적용하지 않는다:

- `upload_index_DataFrame_with_polars` (line 318)
- `upload_index_macro_DataFrame_with_polars` (line 498)
- `upload_stock_timeseries_DataFrame_with_polars` (line 695)
- `upload_etf_constituents_DataFrame_with_polars` (line 1056, 이후 `read_database_uri` 도 동일 uri 재사용)

```python
uri = f"postgresql://{db_user}:{db_password}@127.0.0.1:{port}/quant_data"
```

반면 `download.py`(L108), `tools.py`(L23-25), `refresh_materialized_view_concurrently`(L79-81), `upload_static_variables_DataFrame_with_polars`(L892), `upload_latest_level_with_polars`(L987) 는 모두 `quote_plus(db_password)` 를 적용한다. → 같은 패키지 안에서 비일관.

비밀번호에 URL 예약문자(`@`, `:`, `/`, `#`, `?`)가 있으면 SQLAlchemy URI 파싱이 깨져 접속 실패한다. 실제로 신규 quantdb 계정 비번이 `pw@1` 로 `@` 를 포함한다(이 4개 업로더는 현재 `quant_data` 에만 붙지만, 동일 클래스 비번 규칙이면 quant_data 비번도 특수문자 가능성 있음).

## Proposed approach

4곳 모두 `download.py` 와 동일하게 `quote_plus` 적용:

```python
from urllib.parse import quote_plus  # 이미 upload.py 상단에 import 되어 있음
uri = f"postgresql://{db_user}:{quote_plus(db_password)}@127.0.0.1:{port}/quant_data"
```

`upload.py` 는 이미 `from urllib.parse import quote_plus` 를 import 하고 일부 함수에서 쓰고 있으므로 추가 import 불필요. 4줄만 교체.

## Non-goals

- QuantDB 신규 경로와 무관 (QuantDB 는 처음부터 `quote_plus` 로 작성).
- dbname `quant_data` 하드코딩 등 다른 legacy 이슈는 별도 (quant_data 는 향후 quantdb 로 대체 예정이므로 신규 투자 안 함).
