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
quantdb 배포본의 분석용 long-format 뷰 묶음(`ai_ready.spot_kr_daily`, `ai_ready.prices_daily_usd`, `ai_ready.fx_daily` 등). raw SQL 로 바로 조회. FactSet pivot 스키마와 구조가 달라 기존 fetcher 재사용 불가.
_Avoid_: `ai_ready.etf_timeseries`, `ai_ready.fx` (존재하지 않는 옛 이름)

**Line (상장 라인)**:
가격 시계열이 실제로 매달리는 **정체성 단위**. 식별자는 `tradingitemid`. 한 회사가 분할·개명·재상장을 거치면 라인이 갈린다 — 그때 이름도 티커도 그대로일 수 있다.
_Avoid_: "종목" (한 종목이 여러 라인일 수 있다는 사실을 지운다)

**ISIN**:
증권에 붙는 **라벨**이지 정체성이 아니다. 재발급·재배정되며, 서로 다른 두 **Line** 이 같은 ISIN 을 나눠 갖는 일도 있다(날짜가 겹치는 경우까지 실재). "ISIN 하나 = 종목 하나" 를 가정하면 조용히 틀린다.

**holding / fund**:
FactSet 의 **국가수식 티커** (`AAPL-US`, `SPY-US`). ETF 구성종목 데이터에서 `fund` 는 ETF 자신, `holding` 은 편입 종목을 가리킨다. 국가 접미사가 정체성의 일부다 — 접미사를 뗀 `SPY` 는 다른 것(혹은 아무것도 아닌 것)이다.

**Universe**:
어떤 종목들을 볼 것인가의 집합. ETF 구성종목으로 잡을 때 **어느 시점의 구성인가**가 정의의 일부다 — 특정 월의 스냅샷일 수도, 전 이력의 합집합일 수도 있다. 후자는 생존편향이 없는 대신 "한 번이라도 들어 있었던" 종목을 전부 담는다.

**Membership**:
어느 종목이 **어느 시점에** Universe 안에 있었는가. Universe 가 "누구" 라면 Membership 은 "언제" 다. 전 이력 합집합 Universe 는 Membership 없이는 시점 정보가 없다.

**Span / via**:
ISIN(또는 holding) 을 시점별로 **Line** 에 잇는 매핑에서, `span` 은 그 답이 유효한 날짜 구간이고 `via` 는 **어느 등급이 답했는가** 다. 시점 정확한 등급과 시점을 무시한 폴백 등급이 구분된다 — 폴백으로 답한 구간은 "이 라인이 맞다" 가 아니라 "달리 알 방법이 없다" 는 뜻이다.

## Relationships

- **AI_Quant** 은 **quant_data 배포본** 과 **quantdb 배포본** 두 인스턴스로 존재 (서로 다른 host·dbname·스키마·인증).
- **quantdb 배포본** 이 향후 **quant_data 배포본** 을 완전히 대체 예정 (전환기 동안 둘 공존).
- **quantdb 배포본** 접속은 **Tunnel** + **Service token** 을 모두 요구한다.
- **quant_data 배포본** 접속은 **Tunnel** 만 요구한다 (서비스토큰 없음).
- **ai_ready 스키마** 는 **quantdb 배포본** 에만 있다.
- **ISIN** 은 **Line** 을 가리키는 라벨이며 1:1 이 아니다 — 한 ISIN 이 여러 Line 에 걸칠 수 있고, 그 반대도 시기에 따라 성립한다. 둘을 잇는 것이 **Span / via** 다.
- **Universe** 는 **holding / fund** 로 정의되고, **Membership** 이 거기에 시점을 붙인다.

## 보안 규칙

- **Service token / DB 비밀번호는 라이브러리 코드·배포본(PyPI)·공개 repo 에 절대 들어가지 않는다.** 코드는 env 변수 *이름* 만 참조하고 값은 런타임에 `.env`/env/인자로 주입. (`.env` 는 gitignore 됨.)
- host·dbname 은 시크릿이 아니므로 라이브러리 기본값으로 둘 수 있다.

## Flagged ambiguities

- "ai_quant" 가 단일 DB 처럼 쓰였으나 — 실제로는 두 배포본(quant_data, quantdb)으로 구분됨. 호스트·dbname·스키마·인증이 전부 다름.
