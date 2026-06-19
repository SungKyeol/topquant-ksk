import os
import subprocess
import time
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text

from .tunnel import find_cloudflared

DEFAULT_HOST = "shquantdb.alphawaves.vip"
DEFAULT_DBNAME = "quantdb"
DEFAULT_LOCAL_PORT = 15432


def _make_dsn(db_user, db_password, local_port, dbname):
    return f"postgresql://{db_user}:{quote_plus(db_password)}@127.0.0.1:{local_port}/{dbname}"


def _service_token_env(cf_client_id, cf_client_secret):
    if cf_client_id and cf_client_secret:
        return {
            "TUNNEL_SERVICE_TOKEN_ID": cf_client_id,
            "TUNNEL_SERVICE_TOKEN_SECRET": cf_client_secret,
        }
    return {}


def _tunnel_cmd(cloudflared_exe, hostname, local_port):
    return [cloudflared_exe, "access", "tcp", "--hostname", hostname, "--url", f"127.0.0.1:{local_port}"]


class QuantDB:
    def __init__(self, db_user, db_password, *, hostname=DEFAULT_HOST, dbname=DEFAULT_DBNAME,
                 local_port=DEFAULT_LOCAL_PORT, cf_client_id=None, cf_client_secret=None,
                 tunnel_wait=4.0, connect_timeout=20):
        if not db_user or not db_password:
            raise ValueError("db_user 와 db_password 는 필수입니다 (None/빈 문자열 불가).")
        self.db_user = db_user
        self.db_password = db_password
        self.hostname = hostname
        self.dbname = dbname
        self.local_port = local_port
        self.cf_client_id = cf_client_id if cf_client_id is not None else os.environ.get("CF_ACCESS_CLIENT_ID")
        self.cf_client_secret = (
            cf_client_secret if cf_client_secret is not None else os.environ.get("CF_ACCESS_CLIENT_SECRET")
        )
        self.tunnel_wait = tunnel_wait
        self.connect_timeout = connect_timeout
        self._engine = None
        self._tunnel = None

    @property
    def engine(self):
        if self._engine is None:
            raise RuntimeError("QuantDB 는 `with QuantDB(...) as db:` 컨텍스트 안에서 사용하세요.")
        return self._engine

    def read_sql(self, sql, params=None):
        with self.engine.connect() as conn:
            res = conn.execute(text(sql), params or {})
            return pd.DataFrame(res.fetchall(), columns=list(res.keys()))

    def __enter__(self):
        self._tunnel = self._start_tunnel()
        dsn = _make_dsn(self.db_user, self.db_password, self.local_port, self.dbname)
        self._engine = self._create_verified_engine(dsn)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
        self._kill_tunnel()
        return False

    def _start_tunnel(self):
        exe = find_cloudflared()
        if exe is None:
            raise RuntimeError("cloudflared 실행파일을 찾을 수 없습니다.")
        env = dict(os.environ, **_service_token_env(self.cf_client_id, self.cf_client_secret))
        proc = subprocess.Popen(
            _tunnel_cmd(exe, self.hostname, self.local_port),
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env,
        )
        time.sleep(self.tunnel_wait)
        return proc

    def _kill_tunnel(self):
        if self._tunnel is None:
            return
        subprocess.run(
            ["taskkill", "/F", "/T", "/PID", str(self._tunnel.pid)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        self._tunnel = None

    def _create_verified_engine(self, dsn, max_retries=3, retry_delay=1):
        last_err = None
        for attempt in range(max_retries):
            engine = create_engine(
                dsn, pool_pre_ping=True, connect_args={"connect_timeout": self.connect_timeout}
            )
            try:
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                return engine
            except Exception as e:
                engine.dispose()
                last_err = e
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        raise last_err
