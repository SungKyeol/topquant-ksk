# topquant-ksk — DB 접근 컨텍스트

`topquant_ksk.db` 가 원격 quant 데이터베이스에 어떻게 접속·조회하는지를 다루는 컨텍스트.
"어느 데이터 소스에 붙는가"가 핵심 도메인 구분이다.

## Language

**AI_Quant**:
topquant_ksk 가 접속하는 원격 quant 데이터베이스 *배포본*. 현재 두 개가 존재한다 (아래 둘).
_Avoid_: "the DB", "서버" (어느 배포본인지 모호해짐)

**quant_data 배포본** (기존, **legacy — 전환기 동안만 유지**):
host `db.alphawaves.vip` / dbname `quant_data`. FactSet 파생 스키마(`public`/`private`, `sedol`·`ticker`·`company_name`·`item_name` pivot). 기존 `DBConnection` 의 high-level fetcher 가 붙는 대상. **향후 quantdb 배포본으로 대체 예정** → 신규 투자 안 함.

**quantdb 배포본** (신규, 사용자가 말한 "다른 ai_quant", **향후 기본 배포본**):
host `shquantdb.alphawaves.vip` / dbname `quantdb`. `ai_ready` 스키마. Cloudflare Access **서비스토큰** 보호. 이번 작업의 대상이며, 전환 완료 후 quant_data 를 완전히 대체한다.

**Tunnel**:
`cloudflared access tcp` 로 원격 Postgres 를 `127.0.0.1:15432` 로 끌어오는 보안 프록시. 함수/세션이 직접 띄우고 종료한다.

**Service token**:
Cloudflare Access 서비스토큰(`CF_ACCESS_CLIENT_ID`/`CF_ACCESS_CLIENT_SECRET` → cloudflared 의 `TUNNEL_SERVICE_TOKEN_ID`/`TUNNEL_SERVICE_TOKEN_SECRET`). CF-Access 로 보호된 hostname 에 헤드리스 인증할 때 필요. quantdb 배포본 접속에 필수.

**ai_ready 스키마**:
quantdb 배포본의 분석용 long-format 테이블군(`ai_ready.etf_timeseries`, `ai_ready.fx` 등). raw SQL 로 바로 조회. FactSet pivot 스키마와 구조가 달라 기존 fetcher 재사용 불가.

## Relationships

- **AI_Quant** 은 **quant_data 배포본** 과 **quantdb 배포본** 두 인스턴스로 존재 (서로 다른 host·dbname·스키마·인증).
- **quantdb 배포본** 이 향후 **quant_data 배포본** 을 완전히 대체 예정 (전환기 동안 둘 공존).
- **quantdb 배포본** 접속은 **Tunnel** + **Service token** 을 모두 요구한다.
- **quant_data 배포본** 접속은 **Tunnel** 만 요구한다 (서비스토큰 없음).
- **ai_ready 스키마** 는 **quantdb 배포본** 에만 있다.

## 보안 규칙

- **Service token / DB 비밀번호는 라이브러리 코드·배포본(PyPI)·공개 repo 에 절대 들어가지 않는다.** 코드는 env 변수 *이름* 만 참조하고 값은 런타임에 `.env`/env/인자로 주입. (`.env` 는 gitignore 됨.)
- host·dbname 은 시크릿이 아니므로 라이브러리 기본값으로 둘 수 있다.

## Flagged ambiguities

- "ai_quant" 가 단일 DB 처럼 쓰였으나 — 실제로는 두 배포본(quant_data, quantdb)으로 구분됨. 호스트·dbname·스키마·인증이 전부 다름.
