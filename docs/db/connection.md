# 연결 설정

## DBConnection

DB 모듈의 진입점입니다. `download`, `upload`, `tools` 서브모듈에 대한 접근을 제공합니다.

```python
from topquant_ksk.db import DBConnection

conn = DBConnection(
    db_user="user",
    db_password="pw",
    local_host=False,  # True: localhost:5432 직접 연결, False: Cloudflare 터널
)
```

| 파라미터 | 타입 | 기본값 | 설명 |
|----------|------|--------|------|
| `db_user` | str | - | DB 사용자명 |
| `db_password` | str | - | DB 비밀번호 |
| `local_host` | bool | `False` | `True`면 터널 없이 localhost 직접 연결 |

## 서브모듈 접근

```python
conn.download   # 데이터 조회 함수
conn.upload     # 데이터 업로드 함수
conn.tools      # DB 유틸리티
```

## 연결 방식

### Cloudflare 터널 (기본)

`local_host=False`일 때 `cloudflared`를 통해 보안 터널을 자동으로 생성합니다.

- 터널 포트: `127.0.0.1:15432`
- 함수 호출 시 자동으로 터널 열기 → 쿼리 실행 → 터널 종료
- 연결 실패 시 최대 3회 자동 재시도 (1초 간격)

### 로컬 연결

`local_host=True`일 때 `localhost:5432`로 직접 연결합니다. DB 서버가 로컬에 있을 때 사용합니다.
