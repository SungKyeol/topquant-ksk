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

## QuantDB (quantdb 배포본 / ai_ready 스키마)

`quantdb` 배포본(`ai_ready` 스키마)에 cloudflared 터널로 접속해 raw SQL 을 실행하는 컨텍스트매니저. 기존 `DBConnection`/fetcher 와 별개의 forward 경로입니다 (ADR-0001).

설정은 **인자로 직접 받습니다** (클래스는 `os.environ` 을 내부에서 읽지 않음). 필수는 credential(`db_user`/`db_password`)뿐이고 나머지는 기본값 + override.

```python
import os
from topquant_ksk.db import QuantDB, load_env

load_env()  # repo 루트 .env 로드 (OS 환경변수와 충돌 시 .env 우선 + 경고; 값은 로그에 안 찍힘)
with QuantDB(db_user=os.environ.get("DB_USER"),
             db_password=os.environ.get("DB_PASSWORD")) as db:
    df = db.read_sql(
        "SELECT date, adj_close_pr FROM ai_ready.etf_timeseries "
        "WHERE ticker = :t AND date >= :since ORDER BY date",
        {"t": "069500.KS", "since": "2024-01-01"},
    )
```

- 터널을 1회 열고 컨텍스트 안에서 `read_sql` 을 여러 번 호출 → `__exit__` 에서 터널·엔진 자동 정리.
- `db.engine` 으로 SQLAlchemy 엔진에 직접 접근 가능 (비-SELECT/고급 용도).
- `load_env()`: `.env` → `os.environ`. `override=True`(기본) = .env 우선, 충돌 시 **키 이름만** 경고(값 노출 X). `python-dotenv`(`[db]` extra) 필요.

| 인자 | 기본값 | 설명 |
|------|--------|------|
| `db_user`, `db_password` | - | **필수** (credential) |
| `dbname` | `quantdb` | 데이터베이스명 |
| `hostname` | `shquantdb.alphawaves.vip` | 터널 public hostname |
| `local_port` | `15432` | 로컬 포워더 포트 |
| `cf_client_id` / `cf_client_secret` | `None` | CF Access 서비스토큰 (선택; 현재 shquantdb 는 불필요 — ADR-0002) |
| `cloudflared_bin` | `None` | cloudflared 경로 (없으면 자동탐지) |
| `tunnel_wait` | `4.0` | 터널 기동 대기(초) |
| `connect_timeout` | `20` | 접속 타임아웃(초) |

`.env` 키: `DB_USER`/`DB_PASSWORD`(필수), `DB_NAME`/`TUNNEL_HOSTNAME`/`TUNNEL_PORT`/`CLOUDFLARED_BIN`/`CF_ACCESS_CLIENT_ID`/`CF_ACCESS_CLIENT_SECRET`(선택). 템플릿은 `.env.example` 참고.
