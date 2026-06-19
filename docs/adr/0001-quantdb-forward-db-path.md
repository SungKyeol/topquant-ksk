# QuantDB 를 forward DB 접근 경로로, quant_data 는 legacy 로 전환

신규 `quantdb` 배포본(`ai_ready` 스키마, Cloudflare Access 서비스토큰 보호)이 향후 기존 `quant_data` 배포본을 완전히 대체한다. 이를 위해 `topquant_ksk.db` 에 컨텍스트매니저 기반 신규 클래스 **`QuantDB`** 를 추가하여 forward 경로로 삼고(기본 타깃 = quantdb, 시크릿은 `.env` 에서만 주입, `read_sql` + `.engine` 노출), 기존 `DBConnection`/high-level fetcher 는 quant_data 전용 **legacy 경로**로 동결한다(신규 기능 투자 없음, 전환 완료 후 제거). `ai_ready` 의 long-format 스키마가 FactSet pivot fetcher 와 구조가 달라 재사용이 불가능하고, 신규 배포본의 서비스토큰 인증을 기존 터널이 지원하지 않으므로, 작동 중인 legacy 코드를 건드리지 않는 additive 신규 클래스가 가장 안전하다.

## Considered Options

- **기존 `DBConnection` 확장** — 기각. `DBConnection` 은 호출마다 터널을 여닫는 무상태 façade라, persistent-터널 컨텍스트매니저를 얹으면 한 클래스에 두 lifecycle 이 공존해 의미가 흐려진다.
- **즉시 repoint (quant_data 폐기)** — 기각. 기존 fetcher/upload 가 아직 사용 중이라 전환기 동안 두 경로가 모두 필요하다.

## Consequences

- 전환기 동안 두 DB 경로가 의도적으로 공존한다. quant_data fetcher 는 동결 상태(버그픽스 외 신규 투자 없음).
- 시크릿(DB 비밀번호, CF 서비스토큰)은 `.env` 에만 존재하고, 라이브러리 코드는 env 변수 *이름* 만 참조한다 (PyPI/공개 repo 노출 방지).
- `QuantDB` 는 target-agnostic(파라미터 override 로 quant_data 에도 접속 가능)이라 전환 완료 시 통합이 쉽다.
