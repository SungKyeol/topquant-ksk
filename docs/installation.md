# 설치

## 기본 설치

```bash
pip install topquant-ksk
```

## 선택적 의존성

```bash
# DB 기능 포함 (SQLAlchemy, psycopg2, polars 등)
pip install topquant-ksk[db]

# 시각화 포함 (matplotlib, seaborn)
pip install topquant-ksk[plot]

# 전체 설치
pip install topquant-ksk[all]
```

## 요구사항

- Python >= 3.10
- 기본 의존성: pandas >= 1.5.0, numpy >= 1.20.0, tqdm

## DB 모듈 사전 준비

DB 모듈 사용 시 [Cloudflare Tunnel](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/) 클라이언트가 필요합니다.

```bash
# Windows (winget)
winget install Cloudflare.cloudflared

# 또는 직접 다운로드 후 설치
```

!!! note
    `cloudflared`가 설치되어 있지 않으면 라이브러리가 자동으로 설치를 시도합니다.
