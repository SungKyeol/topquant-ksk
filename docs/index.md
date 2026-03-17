# topquant-ksk

기관 투자자를 위한 파이썬 퀀트 투자 백테스팅 및 분석 도구

## 주요 기능

- **DB 연동** - Cloudflare 터널을 통한 보안 DB 접속, 시계열/마스터 데이터 조회 및 업로드
- **데이터 로딩** - FactSet, DataGuide 등 금융 데이터 소스의 Excel/CSV 파일 로딩
- **위험/수익 분석** - CAGR, Sharpe, MDD, Tracking Error 등 종합 성과 지표
- **포트폴리오 도구** - 리밸런싱 시뮬레이션, 일일 비중/수익률 계산
- **시각화** - 히트맵 기반 성과 분석 차트

## 패키지 구조

```
topquant_ksk/
├── load_data.py            # FactSet/DataGuide 파일 로딩
├── tools.py                # 유틸리티 (수익률 계산, 리샘플링, 포트폴리오)
├── risk_return_metrics.py  # 위험/수익 지표
├── plot.py                 # 히트맵 시각화
└── db/
    ├── connection.py       # DBConnection 클래스
    ├── download.py         # DB fetch 함수
    ├── upload.py           # DB upload 함수
    ├── tools.py            # DB 유틸리티
    └── tunnel.py           # Cloudflare 터널
```

## 빠른 시작

```python
from topquant_ksk.db import DBConnection

conn = DBConnection(db_user="user", db_password="pw")

# 주식 시계열 데이터 조회
df = conn.download.fetch_timeseries_table(
    table_name="public.daily_adjusted_time_series_data_stock",
    item_names=['close_pr', 'close_tr'],
    start_date='2020-01-01',
)
```
