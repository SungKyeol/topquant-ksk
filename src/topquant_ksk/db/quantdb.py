import glob
import hashlib
import json
import os
import pickle
import subprocess
import time
import warnings
from datetime import date, datetime
from typing import NamedTuple
from urllib.parse import quote_plus

import pandas as pd
import connectorx as cx
from sqlalchemy import create_engine, text

from .tunnel import CLOUDFLARED_INSTALL_HELP, ensure_cloudflared

DEFAULT_DBNAME = "quantdb"
DEFAULT_HOSTNAME = "shquantdb.alphawaves.vip"
DEFAULT_LOCAL_PORT = 15432
DEFAULT_POSTGRES_PORT = 5432  # local_host=True 일 때 직결할 로컬 Postgres 포트
# 기본값은 None = 상한을 걸지 않는다. **의도적이다** — 상한의 소유자는 서버다.
# quantdb 는 postgresql.conf 로 전역 statement_timeout(현재 30분)을 걸고 있고(ADR-0050),
# libpq 의 `options` 로 DSN 에 값을 실으면 그건 `source=client` 라 **전역 conf 를 이긴다**.
# 즉 여기에 기본값을 박으면 이 라이브러리로 붙는 모든 소비자가 서버 정책에 구멍을 낸다.
# 이 인자는 정당하게 긴 배치가 상한을 *올릴* 때만 쓴다 (statement_timeout=7200 등).
DEFAULT_STATEMENT_TIMEOUT = None
# 인트라데이(ts) 패널의 표시 타임존. connectorx 가 timestamptz 를 naive-UTC 로 떨구므로
# SQL 에서 이 TZ 의 wall-clock 으로 투영한다 (모든 ts 패널이 KR 장중 데이터 → KST).
INTRADAY_TZ = "Asia/Seoul"
# fetch_timeseries 가 돌려주는 columns MultiIndex 의 모양 버전. 단수/구성이 바뀌면 올린다.
# 1 = (item, ticker, name, isin) / 2 = (item, ticker, name, isin, tradingitemid)  [0.2.0]
PANEL_SHAPE_VER = 2
# fetch_etf_universe_panel 이 돌려주는 EtfUniversePanel(panels + membership)의 모양 버전.
# membership 컬럼 단수/구성이나 NamedTuple 필드가 바뀌면 올린다 — 옛 pkl 이 새 코드에 히트하는 것을 막는다.
UNIVERSE_SHAPE_VER = 6
# 바깥(유니버스) 캐시의 파일 prefix. 정리 glob 이 prefix 단위라 fetch_timeseries 의
# 'ai_ready_*' 캐시와 서로를 지우지 않는다 (relation 이름은 항상 'ai_ready_' 로 시작한다).
UNIVERSE_CACHE_PREFIX = "etf_universe_panel"


def _make_dsn(db_user, db_password, local_port, dbname, statement_timeout=DEFAULT_STATEMENT_TIMEOUT):
    """접속 DSN. statement_timeout(초, None=무제한)은 libpq `options` 로 DSN 에 심는다.

    DSN 에 심는 이유: connectorx(_bulk_read)가 sqlalchemy engine 을 우회해 이 DSN 으로 자기 커넥션을
    직접 열기 때문에, engine 의 connect_args 나 접속 후 `SET` 으로는 fetch_timeseries 경로(대용량을
    끌어오는 바로 그 경로)에 적용되지 않는다. DSN 이 두 드라이버를 동시에 덮는 유일한 지점이다.

    형태 주의: connectorx 의 URL 파서는 공백이 든 `-c name=value` 를 못 먹는다 (공백을 `+` 로 받아
    `FATAL: unrecognized configuration parameter "+statement_timeout"` 로 접속 자체가 실패).
    공백 없는 `--name=value` 만 양쪽 드라이버에서 동작하며, 그래서 옵션은 하나만 실을 수 있다.
    lock_timeout 등 나머지 상한은 애초에 서버가 소유한다 — quantdb 는 postgresql.conf 에서
    전역으로 건다(ADR-0050). 여기 값을 실으면 `source=client` 라 그 전역값을 덮는다.
    """
    dsn = f"postgresql://{db_user}:{quote_plus(db_password)}@127.0.0.1:{local_port}/{dbname}"
    if statement_timeout is not None:
        dsn += f"?options=--statement_timeout={int(statement_timeout) * 1000}"
    return dsn


def _service_token_env(cf_client_id, cf_client_secret):
    if cf_client_id and cf_client_secret:
        return {
            "TUNNEL_SERVICE_TOKEN_ID": cf_client_id,
            "TUNNEL_SERVICE_TOKEN_SECRET": cf_client_secret,
        }
    return {}


def _tunnel_cmd(cloudflared_exe, hostname, local_port):
    return [cloudflared_exe, "access", "tcp", "--hostname", hostname, "--url", f"127.0.0.1:{local_port}"]


def _cache_sig(full, fields, ids, start, end, filter_by, shape_ver=PANEL_SHAPE_VER):
    """fetch_timeseries 결과를 유일하게 식별하는 짧은 해시.

    relation + fields + ids + start/end + filter_by + 반환모양버전을 정규화(정렬·None 센티넬)해서 hash.
    fields/ids 는 정렬 → 순서만 다른 호출이 같은 캐시를 공유한다.

    filter_by 가 키에 **반드시** 들어가야 한다 — 같은 값 리스트를 다른 컬럼에 거는 두 호출
    (ids=['US0378331005'] 를 ticker 로 / isin 으로)은 결과가 전혀 다른데 나머지 인자가 같다.
    shape_ver 는 columns MultiIndex 단수가 바뀔 때 올린다 — 업그레이드 당일 남아 있던 옛 모양
    pkl 이 새 코드에 히트해서 조용히 옛 패널을 돌려주는 것을 막는다.
    """
    norm = {
        "rel": full,
        "fields": "auto" if fields is None
                  else sorted([fields] if isinstance(fields, str) else fields),
        "ids": "all" if ids is None
               else sorted(str(i) for i in ([ids] if isinstance(ids, str) else ids)),
        "start": "" if start is None else str(start),
        "end": "" if end is None else str(end),
        "filter_by": "" if filter_by is None else str(filter_by),
        "shape": shape_ver,
    }
    blob = json.dumps(norm, sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()[:12]


def _cache_path(prefix, sig, cache_dir="pickle_cache"):
    """`pickle_cache/{prefix}_{sig}_{오늘}.pkl` 경로를 만들고, 같은 prefix 의 과거 날짜 파일을 지운다.

    만료 규칙(당일)의 **유일한 구현**이다 — fetch_timeseries 와 fetch_etf_universe_panel 이 이걸
    같이 쓰기 때문에 두 캐시의 수명이 정의상 같다. 정리 glob 이 prefix 로 좁혀져 있어 한 캐시
    종류가 다른 종류의 파일을 지우지 않는다.
    """
    os.makedirs(cache_dir, exist_ok=True)
    today_str = date.today().strftime("%Y%m%d")
    for f in glob.glob(os.path.join(cache_dir, f"{prefix}_*.pkl")):   # broad: 이 prefix 의 과거 날짜 캐시 정리
        if today_str not in os.path.basename(f):
            os.remove(f)
    return os.path.join(cache_dir, f"{prefix}_{sig}_{today_str}.pkl")


def _load_universe_cache(cache_file):
    """바깥(유니버스) 캐시를 읽는다. 읽을 수 없거나 모양이 낯설면 **None** = 캐시 미스로 친다.

    죽지 않는 것이 중요하다: 이 pkl 은 dict 계약이라 필드가 늘 수 있고, 큰 쓰기가 Ctrl+C 로 잘리면
    반쯤 쓰인 파일이 남는다. 그 파일 하나 때문에 매 호출이 예외로 죽으면 사용자는 원인도 모른 채
    손으로 지워야 한다. 미스로 떨어뜨리면 같은 호출이 파일을 정상값으로 덮어써 스스로 낫는다.
    """
    if not os.path.exists(cache_file):
        return None
    try:
        with open(cache_file, "rb") as fh:
            blob = pickle.load(fh)
    except Exception as e:                    # 잘린 pkl / 못 읽는 클래스 등
        warnings.warn(f"fetch_etf_universe_panel: 캐시를 읽지 못해 새로 조회합니다 ({cache_file}): {e!r}")
        return None
    if not isinstance(blob, dict) or not {"membership", "plan", "warnings", "summary",
                                          "n_isins"} <= set(blob):
        warnings.warn(f"fetch_etf_universe_panel: 캐시 모양이 낯설어 새로 조회합니다 ({cache_file}).")
        return None
    return blob


def _norm_fields(fields):
    """fields(None/str/list)를 캐시 키용으로 정규화. 순서만 다른 호출이 같은 캐시를 쓰게 정렬한다."""
    if fields is None:
        return "auto"
    if isinstance(fields, str):
        return [fields]
    return sorted(fields)


def _universe_cache_sig(relations, funds, fields, start, end,
                        shape_ver=PANEL_SHAPE_VER, universe_ver=UNIVERSE_SHAPE_VER):
    """fetch_etf_universe_panel 결과(EtfUniversePanel)를 유일하게 식별하는 짧은 해시.

    - funds 는 **정렬한다** — 결과가 펀드 합집합(OR)이라 순서가 결과를 바꾸지 않는다.
    - relations 는 **정렬하지 않는다** — _widen_membership 이 같은 브리지 컬럼끼리는 먼저 온
      relation 의 ticker/name 을 채택하므로 순서가 membership 컬럼을 실제로 바꾼다.
    - start/end 는 pd.Timestamp 로 정규화한다 — '2020-01-01' 과 Timestamp('2020-01-01') 은 같은
      창이다. 이 함수가 이미 pd.Timestamp(start) 로 창을 만들므로 Timestamp 가능한 값만 온다.
    - 모양버전이 **둘 다** 들어간다: panels 는 fetch_timeseries 모양(PANEL_SHAPE_VER),
      membership 은 이 함수의 모양(UNIVERSE_SHAPE_VER). 어느 쪽이 바뀌어도 옛 pkl 이 죽는다.
    """
    norm = {
        "rels": [r if "." in r else f"ai_ready.{r}" for r in relations],
        "funds": sorted(funds),
        "fields": ({k: _norm_fields(v) for k, v in fields.items()} if isinstance(fields, dict)
                   else _norm_fields(fields)),
        "start": "" if start is None else pd.Timestamp(start).isoformat(),
        "end": "" if end is None else pd.Timestamp(end).isoformat(),
        "shape": [shape_ver, universe_ver],
    }
    blob = json.dumps(norm, sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()[:12]


def _warn_split_isin(full, wide, identity):
    """한 ISIN 이 둘 이상의 (ticker, name) 으로 쪼개져 컬럼이 갈린 경우를 알린다.

    피벗은 안 깨진다 — 갈라지는 조건이 곧 피벗 키가 유일한 조건이기 때문이다. 그래서 **조용하다**:
    한 종목이 컬럼 둘로 나뉜 채 각 구간 밖은 NaN 이 된다(법인 분할·개명·라인 교체). 쓰는 쪽이
    한 종목 = 한 컬럼을 가정하면 그대로 틀린다. 죽이지 말고 알리는 것이 여기서 할 일이다.
    """
    if "isin" not in identity:
        return
    keys = [c for c in ("ticker", "name") if c in identity]
    if not keys:
        return
    cols = wide.columns.to_frame(index=False)[["isin"] + keys].drop_duplicates()
    n_per_isin = cols.groupby("isin").size()
    split = n_per_isin[n_per_isin > 1]
    if len(split):
        sample = ", ".join(split.index[:5])
        warnings.warn(
            f"fetch_timeseries({full}): ISIN {len(split)}건이 둘 이상의 (ticker, name) 으로 갈려 "
            f"컬럼이 나뉘었습니다 (한 종목 = 한 컬럼이 아님): {sample}"
            + (" ..." if len(split) > 5 else ""))


IDENTITY_ORDER = ("ticker", "name", "isin", "tradingitemid")   # 패널 컬럼의 캐논 순서


def _widen_membership(membership, levels, maps, lines):
    """membership 의 라인 키 → 패널과 같은 식별자 축(MultiIndex)으로 넓힌다.

    - levels: relations 들의 identity 레벨 **합집합**. `isin`/`tradingitemid` 는 합집합에 없어도
      항상 남는다 — 라인의 정체를 읽는 축이기 때문이다 (ADR-0007).
    - lines: 키 -> (isin, tradingitemid). 키는 tradingitemid 이고, 해석이 안 된 ISIN 만 isin 이
      키다 (그 라인의 tradingitemid 는 <NA>). 근거는 ADR-0008.
    - maps: [(브리지 컬럼, 그 relation 의 panel.columns)] — relations 순서. ticker/name 값은
      패널 컬럼에서 끌어오고, 같은 브리지끼리는 먼저 온 relation 이 이긴다. 조회는 tradingitemid
      매핑을 먼저 본다 — 한 ISIN 이 라인 둘로 갈렸을 때 tid 만이 둘을 구분하기 때문이다.
    - 패널에 없는 라인(편입인데 미커버)은 ticker/name 이 NaN 이다 — 그 사실이 컬럼에 그대로 보인다.
    """
    table = {}
    for bridge, cols in maps:
        for tup in cols:
            got = dict(zip(cols.names, tup))              # names = ['item'] + identity
            key = got.get(bridge)
            if key is None:
                continue
            slot = table.setdefault((bridge, key), {})
            for lv in levels:
                if lv in got and lv not in slot:
                    slot[lv] = got[lv]
    wide = {lv: [] for lv in levels}
    for key in membership.columns:
        isin, tid = lines.at[key, "isin"], lines.at[key, "tradingitemid"]
        got = dict(table.get(("tradingitemid", int(tid)), {})) if pd.notna(tid) else {}
        for lv, v in table.get(("isin", isin), {}).items():
            got.setdefault(lv, v)
        # isin 은 호출측(lines)이 이미 tid 의 최신 ISIN 으로 통일해서 넘긴다 — 패널 컬럼도 같은
        # map 으로 덮여 있으므로 두 축의 라벨이 어긋나지 않는다 (ADR-0009).
        got["isin"], got["tradingitemid"] = isin, tid
        for lv in levels:
            wide[lv].append(got.get(lv))
    # tid 레벨은 nullable Int64 다 — isin 키 라인의 <NA> 때문에 float 로 내려가면 패널의 int64 와 안 맞는다.
    arrays = [pd.array(wide[lv], dtype="Int64") if lv == "tradingitemid" else wide[lv] for lv in levels]
    membership.columns = pd.MultiIndex.from_arrays(arrays, names=list(levels))
    return membership.sort_index(axis=1)


def _line_key(tup):
    """라인 튜플을 비교 가능한 키로. tradingitemid 는 <NA> 일 수 있어 그대로 비교하면 TypeError 다."""
    return tuple("" if pd.isna(v) else str(v) for v in tup)


def _canonical_isin(cols, canon):
    """컬럼의 `isin` 레벨을 **그 tid 의 최신 ISIN** 으로 덮는다.

    한 tid 에 ISIN 이 여럿 달린다(재발급). 그냥 두면 패널은 가격 행에 각인된 ISIN(현재 기준,
    ADR-0004)을, membership 은 홀딩스가 그때 적은 ISIN 을 들고 있어 라벨이 어긋난다 — SPY+QQQ
    실측 20건(ACKH/ASN/BUD/TWX/WB ...). 양쪽을 같은 map 으로 덮어야 축이 맞는다.
    tid 가 없는 라인(<NA>)은 자기 isin 을 그대로 둔다.

    ticker/name 의 NaN 은 그대로 둔다 — 벤더가 상장폐지 종목의 ticker 를 NULL 로 두는 탓에
    생기는데(Altaba/TFCF/Viacom 등), float NaN 은 `reindex` 매칭도 `== "AAPL"` 비교도 깨지
    않는다(실측 pandas 2.3.3). 위험한 것은 정수 레벨의 `pd.NA` 뿐이다 — 그쪽만 NA 안전하게 다룬다.

    한 tid 에 ISIN 이 여럿 달린다(재발급). 그냥 두면 패널은 가격 행에 각인된 ISIN(현재 기준,
    ADR-0004)을, membership 은 홀딩스가 그때 적은 ISIN 을 들고 있어 라벨이 어긋난다 — SPY+QQQ
    실측 20건(ACKH/ASN/BUD/TWX/WB ...). 양쪽을 같은 map 으로 덮어야 축이 맞는다.
    tid 가 없는 라인(<NA>)은 자기 isin 을 그대로 둔다.
    """
    if not canon or "tradingitemid" not in cols.names or "isin" not in cols.names:
        return cols
    frame = cols.to_frame(index=False)
    frame["isin"] = [i if pd.isna(t) else canon.get(int(t), i)
                     for t, i in zip(frame["tradingitemid"], frame["isin"])]
    return pd.MultiIndex.from_frame(frame)


def _line_index(tuples, names):
    """라인 튜플 목록 → MultiIndex. tradingitemid 레벨을 nullable Int64 로 **고정**한다.

    매칭 때문이 아니다 — `reindex` 는 NaN 이든 `<NA>` 든 int64↔float64 든 다 맞춘다(실측
    pandas 2.3.3). 고정하는 이유는 **돌려주는 축의 dtype** 이다: `from_tuples` 에 `<NA>` 가 하나만
    섞여도 pandas 가 그 레벨을 float64 로 추론해 tradingitemid 가 2590360.0 으로 보이고,
    ADR-0008 이 Int64 로 잡아 둔 것이 정렬 한 번에 도로 풀린다.
    """
    cols = list(zip(*tuples)) if tuples else [() for _ in names]
    arrays = [pd.array(v, dtype="Int64") if n == "tradingitemid" else list(v)
              for n, v in zip(names, cols)]
    return pd.MultiIndex.from_arrays(arrays, names=names)


def _align_panel_columns(panel, membership, drop_unheld, full, warn):
    """패널 컬럼을 membership 라인 축에 맞춘다.

    - membership 에만 있는 라인은 **전부-NaN 컬럼으로 추가**한다. 가격이 없어도 자리는 있어야
      `panel * membership` 이나 reindex 가 라인 하나를 조용히 잃지 않는다.
    - drop_unheld=True 면 membership 에 없는 컬럼을 버린다. 그런 tid 는 **정의상** 편입된 달을
      자기 span 에 담고 있지 않다 (`month_end <@ span` 의 대우). 다만 가격 행의 tid 전환 시점이
      span 경계와 어긋나면 편입 구간 안의 가격이 그 컬럼에 남아 있을 수 있어(실측 VMRK:
      span 은 2026-08-18 에 넘어가는데 가격은 2026-05-20 에 넘어감) 그런 컬럼은 경고한다.
    """
    if panel.empty:                                   # 0행 패널은 피벗 전이라 컬럼이 identity 가 아니다
        return panel
    items = list(dict.fromkeys(panel.columns.get_level_values("item")))
    names = ["item"] + list(membership.columns.names)
    if list(panel.columns.names) != names:
        # relation 마다 식별자 축이 다르다 — tradingitemid 가 없는 패널(spot_kr_daily 등)은
        # membership 라인 여럿이 같은 라벨로 뭉개지므로 1:1 정렬 자체가 성립하지 않는다.
        warn(f"fetch_etf_universe_panel({full}): 식별자 축이 membership 과 달라 정렬하지 않습니다 "
             f"(패널 {list(panel.columns.names)[1:]} vs membership {names[1:]}).")
        return panel
    mem_keys = {_line_key(c) for c in membership.columns}
    panel_keys = {_line_key(c[1:]) for c in panel.columns}

    target = [(it,) + tuple(c) for it in items for c in membership.columns]
    if not drop_unheld:                               # membership 에 없는 패널 컬럼은 뒤에 붙여 보존
        target += [tuple(c) for c in panel.columns if _line_key(c[1:]) not in mem_keys]
    missing = [c for c in membership.columns if _line_key(c) not in panel_keys]
    if missing:
        warn(f"fetch_etf_universe_panel({full}): 편입 라인 {len(missing)}개가 이 패널에 가격이 없어 "
             f"전부-NaN 컬럼으로 들어갑니다: {[c[-2] for c in missing[:5]]}")
    return panel.reindex(columns=_line_index(target, names))


def _sql_lit(v):
    """connectorx 는 :name 바인드 미지원 → SQL 에 값을 인라인할 때 쓰는 Postgres 문자열 리터럴.

    작은따옴표를 2배로 escape (injection 방어). 식별자(테이블/컬럼명)가 아니라 값에만 쓴다.
    """
    return "'" + str(v).replace("'", "''") + "'"


def _is_date_only(v):
    """end 경계값이 '시각 없는 날짜' 인가.

    True 면 인트라데이(ts) 패널에서 그날 장 끝까지(end-of-day) 포함시킨다 — 'ts <= 날짜' 는
    자정(00:00) 경계라 그날 봉(09:00~)이 통째로 빠진다. datetime(시각 포함) 이나 'HH:MM' 가
    든 문자열은 명시 시각으로 보고 정확 경계(<=)를 유지한다(False).
    """
    if isinstance(v, datetime):       # datetime 은 date 의 하위클래스 → 먼저 검사
        return False
    if isinstance(v, date):
        return True
    return ":" not in str(v)          # 문자열: 시각 표기(:) 없으면 날짜-only


class EtfUniversePanel(NamedTuple):
    """QuantDB.fetch_etf_universe_panel 의 반환값.

    - panels: {relation: wide 패널}. index=시간, columns=MultiIndex(item, ticker, name, isin, tradingitemid).
    - membership: 시점별 편입 여부. index=month_end, 값=bool. columns 는 **패널과 같은 식별자 축**이다 —
      relations 들의 identity 레벨 합집합(+ 항상 남는 isin/tradingitemid), 보통 MultiIndex(ticker, name,
      isin, tradingitemid). ticker/name 은 패널 컬럼에서 끌어오므로 패널에 없는 라인은 NaN 이다
      (SPY+QQQ/start=2015 실측 5건: 상장폐지로 벤더가 ticker 를 비운 3건은 name 만 차고, 미커버 2건은 둘 다 NaN).
      **라인의 키는 tradingitemid 다** (ADR-0008) — ISIN 이 재발급된 종목은 한 컬럼으로 접히고 라벨은
      **그 tid 의 최신 ISIN** 이다. 패널 컬럼도 같은 map 으로 덮으므로 두 축의 라벨이 어긋나지
      않는다 (ADR-0009). 그래서 패널의 isin 레벨은 원본 뷰의 isin 과 다를 수 있다 — tradingitemid
      가 정체성이고 isin 은 라벨이다. 편입 시점 tid 로 해석되지 않은 ISIN 만 isin 이 키이고, 그 라인의
      tradingitemid 는 `<NA>` 다. 그래서 tid 레벨은 nullable `Int64` 이고 **비교는 NA 안전해야 한다** —
      `c[3] == 123` 은 그 라인에서 TypeError 다. `pd.notna(v) and v == x` 를 쓰거나 벡터로는
      `.to_series().eq(x).fillna(False)` 를 써라.

    두 산출물을 잇는 키는 **tradingitemid** 다. 어느 쪽도 상대의 부분집합은 아니다 — 패널은 실제
    가격 행이 있는 tid 만 컬럼이 되고(없으면 탈락), membership 은 span 으로 그 시점의 라인을 고른다.
    SPY+QQQ / start=2015-01-01 실측: 공통 799, membership 에만 2(편입인데 그 relation 에 가격 없음),
    패널에만 1(가격은 있는데 창 안에서 한 번도 span-현재 라인이 아님).
    membership.index 는 **start 이하 마지막 month_end(캐리인 1행)** 부터 시작한다 — 그래야
    `membership.reindex(panel.index, method="ffill")` 가 창 첫 달을 비우지 않는다.
    미커버 종목 목록을 따로 담지 않는 이유는 파생되기 때문이다:

        has_px    = panel.columns[panel.notna().any()]                   # 값이 하나라도 있는 컬럼
        covered   = set(has_px.get_level_values("isin"))
        uncovered = set(membership.columns.get_level_values("isin")) - covered

    컬럼 **존재**로 판정하면 안 된다 — 편입 라인은 가격이 없어도 전부-NaN 컬럼으로 들어간다 (ADR-0009).
    """
    panels: dict
    membership: pd.DataFrame


class QuantDB:
    """quantdb 배포본(ai_ready 스키마)에 cloudflared 터널로 접속하는 컨텍스트매니저.

    설정은 전부 인자로 직접 받는다 (os.environ 을 내부에서 읽지 않음).
    필수: db_user/db_password. dbname/hostname/local_port 등은 기본값이 있고 override 가능.
    local_host=True 면 cloudflared 터널 없이 로컬 Postgres(127.0.0.1:5432) 직결 (quantdb PC 용, 기본 False).
    statement_timeout(초)은 기본이 None — 상한은 서버(postgresql.conf, ADR-0050)가 소유한다.
    값을 주면 DSN 의 libpq `options` 로 실려 **서버 전역값을 덮으므로**, 정당하게 긴
    배치가 상한을 *올릴* 때만 쓴다. read_sql/fetch_timeseries 양쪽에 적용된다.
    `.env` 를 쓰려면 호출자가 직접 읽어 인자로 넘긴다 (python-dotenv 예):

        from dotenv import dotenv_values
        cfg = dotenv_values()
        QuantDB(db_user=cfg["DB_USER"], db_password=cfg["DB_PASSWORD"])
    """

    def __init__(self, db_user, db_password, dbname=DEFAULT_DBNAME, hostname=DEFAULT_HOSTNAME, *,
                 local_host=False, local_port=DEFAULT_LOCAL_PORT, cf_client_id=None, cf_client_secret=None,
                 cloudflared_bin=None, tunnel_wait=4.0, connect_timeout=20,
                 statement_timeout=DEFAULT_STATEMENT_TIMEOUT):
        # 클래스는 credential/설정을 직접 인자로 받는다 (os.environ 을 내부에서 읽지 않음).
        # 실제 필수 입력은 credential(db_user/db_password). dbname/hostname 은 기본값이 있고 override 가능.
        missing = [
            name for name, val in (
                ("db_user", db_user), ("db_password", db_password),
                ("dbname", dbname), ("hostname", hostname),
            ) if not val
        ]
        if missing:
            raise ValueError(f"QuantDB: 필수 credential/설정 누락 또는 빈값: {missing}")
        self.db_user = db_user
        self.db_password = db_password
        self.dbname = dbname
        self.hostname = hostname
        self.local_port = int(local_port)
        self.local_host = bool(local_host)
        self.cf_client_id = cf_client_id
        self.cf_client_secret = cf_client_secret
        self.cloudflared_bin = cloudflared_bin
        self.tunnel_wait = tunnel_wait
        self.connect_timeout = connect_timeout
        self.statement_timeout = statement_timeout
        self._engine = None
        self._tunnel = None
        self._dsn = None

    @property
    def engine(self):
        if self._engine is None:
            raise RuntimeError("QuantDB 는 `with QuantDB(...) as db:` 컨텍스트 안에서 사용하세요.")
        return self._engine

    def read_sql(self, sql, params=None, verbose=True):
        """SQL 조회 → pandas.DataFrame.

        - params: :name 바인딩 딕셔너리 (injection 안전).
        - verbose=True(기본): 결과를 print (셀 잘림 없음). False 면 조용히 DataFrame 만 반환.
        """
        with self.engine.connect() as conn:
            res = conn.execute(text(sql), params or {})
            df = pd.DataFrame(res.fetchall(), columns=list(res.keys()))
        if verbose:
            with pd.option_context("display.max_colwidth", None, "display.max_columns", None,
                                   "display.width", 1000):
                print(df)
        return df

    def _bulk_read(self, sql):
        """connectorx 로 SQL 한 방을 pandas DataFrame 으로 bulk read (대용량 fetch 용).

        connectorx(Rust 멀티코어)가 self._dsn 으로 자기 커넥션을 (이미 떠 있는) 터널에 직접 연다 —
        sqlalchemy verified engine/pool 우회. connectorx 는 :name 바인드 미지원 → 호출측이 값을
        _sql_lit 로 인라인해서 넘긴다.
        """
        if self._dsn is None:
            raise RuntimeError("QuantDB 는 `with QuantDB(...) as db:` 컨텍스트 안에서 사용하세요.")
        return cx.read_sql(self._dsn, sql, return_type="pandas")

    def list_tables(self, schema=None, verbose=True):
        """현재 계정이 실제 SELECT 가능한 테이블/뷰/matview/foreign 목록 → pandas.DataFrame.

        컬럼: schema, name, type(table/view/matview/foreign), columns(컬럼명 전부, 콤마 구분).
        - schema USAGE + object SELECT 를 모두 가진 객체만 (실제 접근가능).
        - system 스키마(pg_*, information_schema, timescaledb 내부) 제외.
        - schema: 지정 시 해당 스키마만. None 이면 접근가능한 전체.
        - verbose=True(기본): 객체별로 이름 + 컬럼 전부를 잘리지 않게 print. False 면 조용히 DataFrame 만 반환.
        """
        sql = """
            SELECT n.nspname AS schema,
                   c.relname AS name,
                   CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'table'
                                  WHEN 'v' THEN 'view'  WHEN 'm' THEN 'matview'
                                  WHEN 'f' THEN 'foreign' END AS type,
                   (SELECT string_agg(a.attname, ', ' ORDER BY a.attnum) FROM pg_attribute a
                    WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped) AS columns
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
              AND n.nspname !~ '^pg_'
              AND n.nspname !~ '^_timescaledb'
              AND n.nspname NOT IN ('information_schema', 'timescaledb_information', 'timescaledb_experimental')
              AND has_schema_privilege(n.oid, 'USAGE')
              AND has_table_privilege(c.oid, 'SELECT')
              AND (:schema IS NULL OR n.nspname = :schema)
            ORDER BY n.nspname, c.relname
        """
        df = self.read_sql(sql, {"schema": schema}, verbose=False)
        if verbose:
            if len(df):
                print(f"접근가능 객체 {len(df)}개:")
                for _, r in df.iterrows():
                    print(f"  [{r['schema']}.{r['name']}] ({r['type']})")
                    print(f"      {r['columns']}")
            else:
                print("접근가능한 객체 없음.")
        return df

    def _relation_columns(self, schema, relname):
        """pg_catalog 로 컬럼명 + typcategory 감지 → DataFrame(column_name, typcat).

        information_schema 는 matview 를 누락하므로 pg_attribute 를 직접 읽는다.
        """
        return self.read_sql(
            """
            SELECT a.attname AS column_name, t.typcategory AS typcat
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_type t ON t.oid = a.atttypid
            WHERE n.nspname = :s AND c.relname = :t AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
            """,
            {"s": schema, "t": relname}, verbose=False)

    def fetch_timeseries(self, relation, fields=None, ids=None, start=None, end=None,
                         *, filter_by=None,
                         save_and_reload_pickle_cache=False, verbose=True):
        """ai_ready timeseries 패널 객체(table/view/matview)를 wide pivot DataFrame 으로 가져온다.

        반환: index=시간(date/ts 자동),
              columns=MultiIndex(item=값필드, ticker, name, isin, tradingitemid)
        (식별자는 객체에 존재하는 것만; fx_daily 는 iso_code/currency_name).

        - relation: ai_ready 객체명. relkind 무관 — table/view/matview/foreign 모두 가능 (pg_catalog 컬럼감지).
        - fields: 값 컬럼 리스트 (None 이면 numeric 측정값 자동 — *id/식별자 제외).
        - ids: 필터할 식별자 값 리스트(단일 값도 가능). None 이면 전체.
        - filter_by: ids 를 걸 컬럼명. None 이면 그 relation 의 기본 엔티티(첫 식별자 = ticker,
          없으면 iso_code). identity 에 없는 컬럼도 지정 가능 — WHERE 절에만 쓰이기 때문이다.
          예) filter_by="isin", filter_by="tradingitemid".
        - start/end: 시간 범위 (시간 컬럼 기준).
        - save_and_reload_pickle_cache=True: pickle_cache/ 에 당일 캐시. 전체 파라미터(relation/fields/
          ids/start/end/filter_by + 반환모양버전) 해시를 키로 — 다른 호출끼리 충돌 없음.
          0행 결과는 캐시하지 않음.
        - verbose=True: fetch 시작시각/완료(걸린시간) + 결과 요약 + 패널 print, 미필터 경고, 캐시 로드/저장. False=조용히 DataFrame 만.

        NOTE: 0행이면 pivot 하지 않은 **flat** empty DataFrame 을 돌려준다 (columns.names=[None]).
        빈 결과에 .columns.get_level_values(...) 를 하면 KeyError 이므로 먼저 `.empty` 를 확인할 것.
        """
        full = relation if "." in relation else f"ai_ready.{relation}"
        schema, relname = full.split(".", 1)

        t_start = time.time()
        if verbose:
            print(f"fetch_timeseries({full}): fetch 시작 {time.strftime('%H:%M:%S')}")

        cache_file = None
        if save_and_reload_pickle_cache:
            cache_file = _cache_path(full.replace(".", "_"),
                                     _cache_sig(full, fields, ids, start, end, filter_by))
            if os.path.exists(cache_file):
                with open(cache_file, "rb") as fh:
                    cached = pickle.load(fh)
                if verbose:
                    print(f"pickle cache load: {cache_file}")
                    print(f"fetch_timeseries({full}): 완료 ({time.time() - t_start:.2f}s)")
                return cached

        cols = self._relation_columns(schema, relname)
        if cols.empty:
            raise ValueError(f"{full}: 컬럼이 없습니다 (테이블/뷰/matview 없음 또는 접근 불가).")
        colnames = list(cols["column_name"])
        numeric_cols = set(cols.loc[cols["typcat"] == "N", "column_name"])

        time_col = "ts" if "ts" in colnames else ("date" if "date" in colnames else None)
        if time_col is None:
            raise ValueError(f"{full}: 시간 컬럼(ts/date) 없음 — timeseries 패널 뷰가 아닙니다. read_sql 을 쓰세요.")
        # tradingitemid 는 **맨 뒤**여야 한다 — identity[0] 이 기본 엔티티(필터 컬럼)이므로,
        # 앞에 두면 기존 ids=["AAPL"] 호출이 tradingitemid 로 걸려 조용히 0행이 된다.
        # 이 컬럼이 identity 에 들어가야 피벗 유일성이 보장된다: (date, tradingitemid) 는
        # 중복이 없지만 (date, ticker, name, isin) 은 없다 — 같은 ISIN 을 두 라인이 나눠 갖고
        # 날짜가 겹치는 경우가 실재하며(EQR/VMRK 60일), 지금은 name 이 달라서 우연히 안 깨진다.
        identity = [c for c in ("ticker", "name", "isin", "tradingitemid") if c in colnames]
        if not identity:                      # ticker 없는 뷰(fx_daily 등)는 iso_code 그룹
            identity = [c for c in ("iso_code", "currency_name") if c in colnames]
        if not identity:
            raise ValueError(f"{full}: 식별자 컬럼(ticker/iso_code 등) 없음.")
        entity = identity[0]
        if filter_by is not None:             # WHERE 절 컬럼만 교체 — 피벗축(identity)은 그대로
            if filter_by not in colnames:
                raise ValueError(f"{full}: filter_by 컬럼 '{filter_by}' 없음. 가능: {colnames}")
            entity = filter_by

        if fields is None:
            value_cols = [c for c in colnames
                          if c in numeric_cols and not c.endswith("id")
                          and c not in identity and c != time_col]
        else:
            value_cols = [fields] if isinstance(fields, str) else list(fields)
        if not value_cols:
            raise ValueError(f"{full}: 값 컬럼이 없습니다 (fields 를 지정하세요).")

        conditions = []                                  # connectorx 는 :name 바인드 미지원 → 값 인라인(_sql_lit)
        n_ids = 0
        if ids is not None:
            idlist = [ids] if isinstance(ids, (str, int)) else list(ids)
            n_ids = len(idlist)
            conditions.append(f"{entity} IN ({', '.join(_sql_lit(t) for t in idlist)})")
            # tradingitemid 를 가진 글로벌 패널(prices_daily_*)을 ticker 로 거는 것은 위험하다:
            # 티커는 나라마다 재사용되므로 같은 문자열의 외국 상장이 조용히 섞여 든다
            # (SPY+QQQ 유니버스 실측: +221 종목 / +787,773 행). tradingitemid 나 isin 을 써라.
            if entity == "ticker" and "tradingitemid" in colnames:
                warnings.warn(
                    f"fetch_timeseries({full}): ticker 로 필터 중 — 이 패널은 여러 나라의 상장을 담고 있어 "
                    f"같은 티커의 외국 종목이 섞일 수 있습니다. filter_by='tradingitemid' 또는 'isin' 권장.")
        if start is not None:
            conditions.append(f"{time_col} >= {_sql_lit(start)}")
        if end is not None:
            if time_col == "ts" and _is_date_only(end):
                # 날짜-only end 를 인트라데이(ts) 패널에서 그날 끝까지 inclusive (다음날 자정 미만).
                # 'ts <= 날짜' 는 자정 경계라 그날 인트라데이 봉이 통째로 빠지므로 금지.
                conditions.append(f"{time_col} < ({_sql_lit(end)}::date + 1)")
            else:
                conditions.append(f"{time_col} <= {_sql_lit(end)}")
        if not conditions:
            warnings.warn(f"fetch_timeseries({full}): tickers/기간 미지정 — 전체 fetch (대용량 위험).")

        # connectorx 는 timestamptz 를 naive-UTC 로 반환 → ts 패널은 KST wall-clock 으로 투영해
        # 09:05~15:30 KST 로 나오게 한다 (일봉 date 패널은 tz 개념 없음 → 그대로).
        time_select = (f"({time_col} AT TIME ZONE '{INTRADAY_TZ}') AS {time_col}"
                       if time_col == "ts" else time_col)
        select = ", ".join([time_select] + identity + value_cols)
        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        long = self._bulk_read(f"SELECT {select} FROM {full}{where} ORDER BY {time_col}")   # connectorx → pandas
        if long.empty:
            if verbose:
                print(f"fetch_timeseries({full}): 0행 (조건에 맞는 데이터 없음).")
                print(f"fetch_timeseries({full}): 완료 ({time.time() - t_start:.2f}s)")
            return long

        long[time_col] = pd.to_datetime(long[time_col])
        # pandas pivot — wide(수만 열) 패널에서 polars melt-pivot 보다 압도적으로 빠름 (벤치: 6.8s vs 773s/13.5M행).
        wide = long.pivot(index=time_col, columns=identity, values=value_cols)
        wide.columns = wide.columns.set_names(["item"] + identity)
        _warn_split_isin(full, wide, identity)
        if verbose:
            filt = f"{entity} IN ({n_ids:,}건)" if ids is not None else "필터없음(전체)"
            print(f"fetch_timeseries({full}): {wide.shape[0]:,}행 x {wide.shape[1]:,}열 "
                  f"[{wide.index.min()} ~ {wide.index.max()}], fields={value_cols}, filter={filt}")
            print(wide)                                  # result 도 print (pandas 기본 잘림 — wide panel 폭주 방지)
        if cache_file is not None:                       # 0행은 위에서 early-return → 비결과 미캐시
            with open(cache_file, "wb") as fh:
                pickle.dump(wide, fh)
            if verbose:
                print(f"pickle cache save: {cache_file}")
        if verbose:
            print(f"fetch_timeseries({full}): 완료 ({time.time() - t_start:.2f}s)")
        return wide

    def fetch_etf_universe_panel(self, relations, funds, fields=None, start=None, end=None, *,
                                 drop_unheld_panel_columns=True,
                                 save_and_reload_pickle_cache=False, verbose=True):
        """글로벌 ETF 의 **전 이력** 구성종목을 유니버스로 잡아 패널(들) + 편입행렬을 함께 가져온다.

        - relations: ai_ready 패널 객체명 (str 또는 리스트). relation 마다 브리지 컬럼을 자동 선택한다 —
          tradingitemid 가 있으면 그것, 없으면 isin, 둘 다 없으면 ValueError.
        - funds: 펀드 식별자. **국가수식 필수** (예: "SPY-US"). bare "SPY" 는 0행이다.
        - fields: 값 컬럼. None(관계별 자동감지) / 리스트(모든 관계에 동일) / {relation: 리스트}.
        - start/end: **셋 다** 에 걸린다 — 유니버스(어떤 종목을 담을지), membership, 패널.
          유니버스/membership 의 하한만 `start` 가 아니라 **start 이하의 마지막 month_end**(캐리인 1행)
          이고 패널은 `start` 그대로다. start=None 이면 전 이력 유니버스(생존편향 없음, 대신 창 밖에서만
          편입됐던 종목까지 전부)다. 근거는 ADR-0006.
        - drop_unheld_panel_columns: 패널 컬럼은 **항상** membership 라인 축을 포함한다 — 가격이
          없는 편입 라인도 전부-NaN 컬럼으로 들어간다. 기본값(True)은 membership 에 없는 패널
          컬럼을 버려 두 축을 **정확히 같게** 만든다 (SPY+QQQ 전 이력 실측 양쪽 1,227). 그런 tid 는
          정의상 편입된 달을 자기 span 에 담고 있지 않다 — `month_end <@ span` 의 대우다 (ADR-0009).
          False 면 그것들을 뒤에 그대로 남긴다(평면 조회 원본 그대로, 실측 1,230).
        - save_and_reload_pickle_cache=True: **두 겹**으로 캐시한다.
          (1) 유니버스/브리지/membership 해석 결과를 `pickle_cache/etf_universe_panel_*.pkl` 에
              (넓혀진 membership + '어느 relation 을 어느 컬럼의 어느 값으로 걸었나' 계획).
              히트하면 그 쿼리 4개를 통째로 건너뛴다 — 안쪽 fetch_timeseries 캐시만으로는 못 하는
              일이다. 그쪽 키에 `ids=tids` 가 들어가고 그 tids 가 바로 이 쿼리들의 **결과**라,
              앞 쿼리를 돌리기 전엔 파일 이름조차 못 만든다. 패널 자체는 여기 담지 않는다 —
              안쪽이 이미 갖고 있어 담으면 하루치 디스크가 두 배(전 이력 실측 229MB)가 된다.
          (2) 패널은 fetch_timeseries 가 자기 캐시로 돌려준다(다른 호출과 공유). 히트 경로는
              그 호출을 계획대로 재생만 하므로 DB 쿼리는 0회다.
          키 = relations(순서 유지) + funds(정렬) + fields + start/end + 모양버전 2개.
          만료는 fetch_timeseries 와 **같은 당일 규칙**(_cache_path 한 곳이 정의). 어떤 패널이든
          0행이거나 membership 이 비면 저장하지 않는다 — 안쪽의 "빈 결과 미캐시" 규칙을 바깥이
          무력화하면 안 되기 때문이다. 히트해도 데이터품질 경고는 다시 발행된다.
        - verbose: fetch_timeseries 에 그대로 전달.

        유니버스는 `row_type='month_end'` 만 쓴다 — 'mtd' 는 진행 중인 달이라 매 평일 덮어쓰인다.
        `kind='security'` 로 좁혀 cash/unidentified 를 뺀다.
        펀드를 여러 개 주면 결과는 **합집합(OR)** 이고, 어느 펀드에서 왔는지는 남지 않는다.
        membership 컬럼은 relations 들의 identity 레벨 합집합으로 넓혀진다 (ADR-0007) — 패널과 같은 축이라
        `panel[item].reindex(columns=membership.columns)` 로 바로 겹칠 수 있다. 라인의 키는 tradingitemid
        이고 해석 실패 ISIN 만 isin 키다 (ADR-0008) — 데이터품질 경고 셋(미해석 ISIN / ISIN 재발급 접힘 /
        시점무시 폴백)이 그때그때 뜬다.

        가격은 tid 목록으로 **평면 조회**하고, span(시점 인지 매핑)은 **membership 에만** 적용한다.
        가격까지 span 으로 조인하면 정체성은 정확해지지만 폴백 등급(via='isin')이 답한 구간에서
        가격이 통째로 탈락한다 (SPY+QQQ 실측: SLM 636행 손실, 소요 2배). 근거는 topquant-ksk ADR-0005.
        """
        rels = [relations] if isinstance(relations, str) else list(relations)
        fundlist = [funds] if isinstance(funds, str) else list(funds)
        t_start = time.time()

        cache_file = None
        if save_and_reload_pickle_cache:
            cache_file = _cache_path(UNIVERSE_CACHE_PREFIX,
                                     _universe_cache_sig(rels, fundlist, fields, start, end))
            blob = _load_universe_cache(cache_file)
            if blob is not None:
                if verbose:
                    print(f"pickle cache load: {cache_file}")
                for msg in blob["warnings"]:      # 데이터품질 경고는 히트에서도 반드시 다시 뜬다 —
                    warnings.warn(msg)            # 조용히 사라지면 "편입 True 인데 패널 NaN" 을 모른 채 쓴다
                panels = {}
                # 패널 자체는 담지 않는다 — 안쪽 캐시가 이미 그 바이트를 갖고 있어서
                # 여기 또 담으면 하루치 디스크가 두 배가 된다(SPY+QQQ 전이력 실측 229MB).
                # 대신 '어느 relation 을 어느 컬럼의 어느 값으로 걸었나'(plan)만 담아 그대로 재생하면
                # 같은 인자 → 같은 안쪽 키 → 안쪽 히트다. 쿼리는 여전히 0회.
                for rel, col, vals in blob["plan"]:
                    fld = fields.get(rel) if isinstance(fields, dict) else fields
                    panel = self.fetch_timeseries(rel, fields=fld, ids=vals, filter_by=col,
                                                  start=start, end=end,
                                                  save_and_reload_pickle_cache=True, verbose=verbose)
                    if panel.empty:               # 안쪽 pkl 이 사라져 다시 조회했는데 0행인 경우
                        full = rel if "." in rel else f"ai_ready.{rel}"
                        warnings.warn(
                            f"fetch_etf_universe_panel({full}): 0행 — 유니버스"
                            f"({blob['n_isins']:,} ISIN)와 이 패널의 교집합이 없습니다.")
                    panels[rel] = _align_panel_columns(
                        panel, blob["membership"], drop_unheld_panel_columns,
                        rel if "." in rel else f"ai_ready.{rel}", warnings.warn)
                if verbose:
                    print(f"{blob['summary']} ({time.time() - t_start:.2f}s)")
                return EtfUniversePanel(panels=panels, membership=blob["membership"])

        warn_msgs = []                            # 캐시에 함께 저장 → 히트 때 그대로 재발행

        def _warn(msg):
            warn_msgs.append(msg)
            warnings.warn(msg)

        # 창(start/end)은 패널뿐 아니라 **유니버스와 membership 에도** 건다. 하한은 start 자체가
        # 아니라 start 이하의 마지막 month_end(=캐리인 1행)다 — 창이 열리는 달의 구성원을 모르면
        # membership.reindex(panel.index).ffill() 이 첫 달을 통째로 비운다. 과거 행이라 선견편향은 없다.
        win, wparams = "", {"f": fundlist}
        if start is not None:
            lo = self.read_sql(
                """
                SELECT max(h.month_end) AS lo FROM ai_ready.etf_global_holdings_monthly h
                WHERE h.fund = ANY(:f) AND h.row_type = 'month_end' AND h.month_end <= :s
                """, {"f": fundlist, "s": pd.Timestamp(start).date()}, verbose=False)["lo"][0]
            if pd.notna(lo):                     # start 앞에 홀딩스가 없으면 하한 없이 처음부터
                win += " AND h.month_end >= :lo"
                wparams["lo"] = lo
        if end is not None:
            win += " AND h.month_end <= :hi"
            wparams["hi"] = pd.Timestamp(end).date()

        universe = self.read_sql(
            f"""
            SELECT DISTINCT h.holding_isin AS isin
            FROM ai_ready.etf_global_holdings_monthly h
            WHERE h.fund = ANY(:f) AND h.row_type = 'month_end' AND h.kind = 'security'
              AND h.holding_isin IS NOT NULL{win}
            """, wparams, verbose=False)
        isins = universe["isin"].tolist()
        if not isins:
            raise ValueError(
                f"유니버스가 비었습니다 (funds={fundlist}, start={start}, end={end}). fund 는 국가수식이어야 "
                f"합니다 — 'SPY' 가 아니라 'SPY-US'. 창이 홀딩스 기간(월말) 밖이어도 비어 있을 수 있습니다.")

        bridge = self.read_sql(
            """
            SELECT DISTINCT tradingitemid FROM ai_ready.isin_tradingitem
            WHERE isin = ANY(:i) AND tradingitemid IS NOT NULL
            """, {"i": isins}, verbose=False)
        tids = bridge["tradingitemid"].astype("int64").tolist()

        # tid 하나에 ISIN 이 여럿이면 라벨을 **최신 ISIN** 으로 통일한다 (ADR-0009). 시점 인지
        # 등급(isin_span)을 폴백(via='isin')보다 우선하고, 그 안에서 열린 span > 늦게 시작한 span.
        canon = self.read_sql(
            """
            SELECT DISTINCT ON (v.tradingitemid) v.tradingitemid, v.isin
            FROM ai_ready.isin_tradingitem v
            WHERE v.tradingitemid = ANY(:t)
            ORDER BY v.tradingitemid, (v.via = 'isin_span') DESC,
                     (upper(v.span) IS NULL) DESC, lower(v.span) DESC NULLS LAST
            """, {"t": tids}, verbose=False)
        canon_isin = dict(zip(canon["tradingitemid"].astype("int64"), canon["isin"]))

        # membership: (isin, month_end) -> 그 시점의 tradingitemid. span 이 서로 겹치지 않아 1행이다.
        mem = self.read_sql(
            f"""
            SELECT DISTINCT h.month_end, h.holding_isin AS isin, v.tradingitemid, v.via
            FROM ai_ready.etf_global_holdings_monthly h
            LEFT JOIN ai_ready.isin_tradingitem v
                   ON v.isin = h.holding_isin AND h.month_end <@ v.span
            WHERE h.fund = ANY(:f) AND h.row_type = 'month_end' AND h.kind = 'security'
              AND h.holding_isin IS NOT NULL{win}
            """, wparams, verbose=False)

        fb = sorted(set(mem.loc[mem["tradingitemid"].isna(), "isin"]))
        if fb:
            _warn(f"fetch_etf_universe_panel: {len(fb)}개 ISIN 이 편입 시점의 tradingitemid 로 해석되지 않아 "
                  f"**isin 을 키로** 남깁니다 (tradingitemid 레벨 <NA>, "
                  f"{int(mem['tradingitemid'].isna().sum()):,}행): {fb[:5]}"
                  + (" ..." if len(fb) > 5 else ""))
        # via='isin' 은 시점무시 폴백 등급이다 — 그 구간의 tid 는 실제 그 시기 가격을 갖지 않을 수 있다.
        n_fb = int((mem["via"] == "isin").sum())
        if n_fb:
            _warn(f"fetch_etf_universe_panel: membership {n_fb:,}행이 시점무시 폴백(via='isin')으로 해석됐습니다 "
                  f"— 그 구간은 편입 True 인데 패널이 NaN 일 수 있습니다.")

        ok = mem.copy()
        ok["tradingitemid"] = ok["tradingitemid"].astype("Int64")     # nullable — 미해석 라인은 <NA>
        # 패널 index 는 fetch_timeseries 가 to_datetime 한다 — membership 도 맞춰야 정렬이 된다.
        ok["month_end"] = pd.to_datetime(ok["month_end"])
        ok["_in"] = True
        # 라인의 키는 tradingitemid 다 (ADR-0008). ISIN 이 재발급된 종목은 홀딩스가 시기별로 다른
        # ISIN 을 들지만 tid 는 하나라, isin 을 키에 넣으면 같은 라인이 둘로 갈린다 (실측 VMED).
        ok["_key"] = [f"T{t}" if pd.notna(t) else f"I{i}"
                      for t, i in zip(ok["tradingitemid"], ok["isin"])]
        both = set(ok.loc[ok["tradingitemid"].isna(), "isin"]) & set(
            ok.loc[ok["tradingitemid"].notna(), "isin"])
        if both:
            _warn(f"fetch_etf_universe_panel: {len(both)}개 ISIN 이 어떤 달은 tid 로, 어떤 달은 isin 으로 "
                  f"해석돼 한 종목이 두 컬럼이 됩니다: {sorted(both)[:5]}")
        # notna() 로 바로 bool 을 만든다 — fillna(False).astype(bool) 은 object dtype 을 조용히
        # downcast 해서 pandas FutureWarning 을 낸다.
        membership = (ok.drop_duplicates(["month_end", "_key"])   # 펀드 겹침(SPY∩QQQ)이 낳은 중복 행만
                        # 접는다 — 결과는 펀드 합집합(OR)이고 어느 펀드였는지는 남지 않는다.
                        .pivot(index="month_end", columns="_key", values="_in")
                        .notna().sort_index())
        # 키의 라벨(isin/tid). 한 tid 가 ISIN 둘로 편입됐으면 그 tid 의 **최신 ISIN** 으로 통일한다 —
        # 패널 컬럼도 같은 map 으로 덮으므로 두 축의 라벨이 정의상 같아진다 (ADR-0009).
        lines = (ok.sort_values("month_end").drop_duplicates("_key", keep="last")
                   .set_index("_key")[["isin", "tradingitemid"]])
        lines["isin"] = [i if pd.isna(t) else canon_isin.get(int(t), i)      # 패널과 같은 라벨로
                         for t, i in zip(lines["tradingitemid"], lines["isin"])]
        folded = (ok.dropna(subset=["tradingitemid"]).drop_duplicates(["_key", "isin"])
                    .groupby("_key")["isin"].nunique())
        folded = folded[folded > 1]
        if len(folded):
            sample = {k: sorted(ok.loc[ok["_key"] == k, "isin"].unique()) for k in folded.index[:3]}
            _warn(f"fetch_etf_universe_panel: {len(folded)}개 tradingitemid 가 ISIN 둘 이상으로 편입돼 "
                  f"한 컬럼으로 접혔습니다 (라벨은 마지막 편입 시점 ISIN): {sample}")

        panels, ident_union, maps, plan = {}, [], [], []
        for rel in rels:
            full = rel if "." in rel else f"ai_ready.{rel}"
            schema, relname = full.split(".", 1)
            colnames = list(self._relation_columns(schema, relname)["column_name"])
            if "tradingitemid" in colnames:
                col, vals = "tradingitemid", tids
            elif "isin" in colnames:
                col, vals = "isin", isins
            else:
                raise ValueError(
                    f"{full}: 유니버스로 주소지정 불가 — tradingitemid/isin 컬럼이 둘 다 없습니다.")
            plan.append((rel, col, vals))         # 캐시 히트 때 이대로 재생 → 안쪽 캐시가 패널을 준다
            for c in (c for c in IDENTITY_ORDER if c in colnames and c not in ident_union):
                ident_union.append(c)             # relations 들의 identity 레벨 합집합
            fld = fields.get(rel) if isinstance(fields, dict) else fields
            panel = self.fetch_timeseries(rel, fields=fld, ids=vals, filter_by=col,
                                          start=start, end=end,
                                          save_and_reload_pickle_cache=save_and_reload_pickle_cache,
                                          verbose=verbose)
            if panel.empty:
                _warn(f"fetch_etf_universe_panel({full}): 0행 — 유니버스({len(isins):,} ISIN)와 이 패널의 "
                      f"교집합이 없습니다 (예: 글로벌 유니버스 × 한국 패널).")
            else:                                 # 0행 패널은 피벗 전이라 컬럼이 identity 가 아니다
                panel = panel.copy()
                panel.columns = _canonical_isin(panel.columns, canon_isin)
                maps.append((col, panel.columns))
            panels[rel] = panel

        levels = [c for c in IDENTITY_ORDER if c in ident_union or c in ("isin", "tradingitemid")]
        membership = _widen_membership(membership, levels, maps, lines)
        # 축 맞추기는 membership 이 완성된 뒤다 — 패널 컬럼이 membership 라인을 기준으로 재배치된다.
        for rel in list(panels):
            panels[rel] = _align_panel_columns(
                panels[rel], membership, drop_unheld_panel_columns,
                rel if "." in rel else f"ai_ready.{rel}", _warn)

        out = EtfUniversePanel(panels=panels, membership=membership)
        # 요약은 verbose 와 무관하게 만든다 — verbose=False 로 채운 캐시를 verbose=True 로 히트할 때
        # 출력할 문구가 캐시 안에 있어야 한다 (히트 시점엔 isins/tids/wparams 가 이미 없다).
        summary = (f"fetch_etf_universe_panel: funds={fundlist}, "
                   f"창=[{wparams.get('lo', '처음')} ~ {wparams.get('hi', '끝')}], 유니버스 {len(isins):,} ISIN "
                   f"-> tradingitemid {len(tids):,}, membership {membership.shape[0]:,}개월 x "
                   f"{membership.shape[1]:,}라인, relations={rels}")
        # 빈 결과는 저장하지 않는다: fetch_timeseries 가 0행을 캐시하지 않는 이유(transient empty 가
        # 하루 고정되는 것 방지)가 여기서도 그대로고, 바깥이 이기면 안쪽 규칙이 무력화되기 때문이다.
        if cache_file is not None and not membership.empty and not any(p.empty for p in panels.values()):
            with open(cache_file, "wb") as fh:
                pickle.dump({"membership": membership, "plan": plan, "warnings": warn_msgs,
                             "summary": summary, "n_isins": len(isins)}, fh)
            if verbose:
                print(f"pickle cache save: {cache_file}")
        if verbose:
            print(f"{summary} ({time.time() - t_start:.2f}s)")
        return out

    def __enter__(self):
        if not self.local_host:                       # local_host 면 터널 없이 로컬 Postgres 직결
            self._tunnel = self._start_tunnel()
        try:
            port = DEFAULT_POSTGRES_PORT if self.local_host else self.local_port
            dsn = _make_dsn(self.db_user, self.db_password, port, self.dbname, self.statement_timeout)
            self._dsn = dsn                           # connectorx bulk read 가 재사용 (sqlalchemy engine 우회)
            self._engine = self._create_verified_engine(dsn)
        except Exception:
            # 엔진 생성 실패 시 __exit__ 가 호출되지 않으므로 여기서 터널을 정리한다.
            self._kill_tunnel()
            raise
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
        self._kill_tunnel()
        return False

    def _start_tunnel(self):
        exe = self.cloudflared_bin or ensure_cloudflared()
        if not exe:
            raise RuntimeError(
                "cloudflared 실행파일을 찾을 수 없습니다 (winget 자동 설치도 실패). 설치 방법:\n"
                + CLOUDFLARED_INSTALL_HELP
            )
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
