# QuantDB.__enter__ 가 엔진 생성 실패 시 cloudflared 터널 누수

**Date raised**: 2026-06-24
**Source**: ai_ready 조회 중 원격 DB 다운으로 연결 실패 → 실패할 때마다 cloudflared 프로세스가 남는 것을 발견 (`src/topquant_ksk/db/quantdb.py` `__enter__`)
**Status**: Done
**Closed**: 2026-06-24
**Resolution**: `__enter__` 의 엔진 생성을 try/except 로 감싸 실패 시 `_kill_tunnel()` 후 re-raise. network 불필요 단위테스트 `test_enter_failure_kills_tunnel` 추가 (commit 1a51d00).

## Motivation

`QuantDB.__enter__` 는 `_start_tunnel()` 로 cloudflared 를 띄운 뒤 `_create_verified_engine()` 로 접속을 검증한다. 그런데 엔진 검증이 실패(예: 원격 다운 → `psycopg2.OperationalError: timeout`)하면 `__enter__` 가 예외를 던지며 **컨텍스트에 진입하지 못한다**. 파이썬은 `__enter__` 가 raise 하면 `__exit__` 를 호출하지 않으므로 `_kill_tunnel()` 이 돌지 않고, `_start_tunnel()` 이 띄운 cloudflared 프로세스가 그대로 남는다.

→ 연결 실패가 반복되면 cloudflared 프로세스가 계속 쌓이고, 127.0.0.1:15432 포트 점유/경합으로 후속 시도까지 방해할 수 있다.

현재 코드:
```python
def __enter__(self):
    self._tunnel = self._start_tunnel()
    dsn = _make_dsn(self.db_user, self.db_password, self.local_port, self.dbname)
    self._engine = self._create_verified_engine(dsn)   # 여기서 raise 하면 _tunnel 누수
    return self
```

## Proposed approach

`__enter__` 안에서 엔진 생성을 try/except 로 감싸 실패 시 터널을 정리하고 re-raise:

```python
def __enter__(self):
    self._tunnel = self._start_tunnel()
    try:
        dsn = _make_dsn(self.db_user, self.db_password, self.local_port, self.dbname)
        self._engine = self._create_verified_engine(dsn)
    except Exception:
        self._kill_tunnel()
        raise
    return self
```

테스트(네트워크 불필요): `_start_tunnel` 을 가짜로, `_create_verified_engine` 가 raise 하도록 monkeypatch 한 뒤 `with QuantDB(...)` 가 예외를 던지면서 `_kill_tunnel` 이 호출됐는지 assert.

## Non-goals

- `_create_verified_engine` 의 재시도(3회) 로직은 그대로.
- 원격 다운 자체는 인프라 문제(이 노트 범위 밖).
