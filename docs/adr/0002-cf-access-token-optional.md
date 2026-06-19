# Cloudflare Access 서비스토큰을 QuantDB 의 선택적 인증으로 (shquantdb 는 현재 미강제)

원본 `series_kodex_spy_krw_tunnel.py` 는 `shquantdb.alphawaves.vip` 가 Cloudflare Access 로 보호된다고 보고, 서비스토큰(`CF_ACCESS_CLIENT_ID`/`CF_ACCESS_CLIENT_SECRET` → cloudflared 의 `TUNNEL_SERVICE_TOKEN_ID`/`TUNNEL_SERVICE_TOKEN_SECRET`)을 **필수**로 강제했다(없으면 `RuntimeError`, docstring "shquantdb 는 Cloudflare Access 보호이므로 ... 주입"). 그러나 2026-06-19 실측 결과 `cloudflared access tcp` 로 **서비스토큰 없이** 접속하면 `SELECT 1` 뿐 아니라 `ai_ready.etf_timeseries`/`ai_ready.fx` 실데이터 쿼리까지 정상 반환된다(슈퍼유저 기준) — 즉 이 hostname 에는 현재 Access 정책이 강제되지 않는다. 따라서 **QuantDB 는 cf_client_id/secret 가 모두 주어진 경우에만 서비스토큰을 주입하고, 없으면 plain 터널을 연다 — 토큰은 필수가 아니라 선택 설정**으로 둔다. 실제 동작과 일치하고, 라이브러리를 다른(보호되지 않은) 호스트에도 쓸 수 있게 하며, 토큰이 있으면 주입되므로 향후 Access 가 켜져도 코드 변경 없이 대응된다.

## Considered Options

- **서비스토큰 필수 (원본 방식)** — 기각. 실측상 불필요하고, Access 없는 호스트에는 접속 자체를 막으며(이식성 저하), 잘못된 가정을 코드에 박는다.

## Consequences

- 토큰은 `.env`/`.env.example` 에서 선택 항목. 비워도 접속된다.
- 향후 hostname 에 Access 정책이 켜지면 `.env` 에 토큰만 채우면 된다(이미 있으면 주입됨) — 코드 변경 불필요.
- **mandatory 토큰 가드를 다시 도입하지 말 것** — 현재 미강제임이 실측으로 확인됨.
