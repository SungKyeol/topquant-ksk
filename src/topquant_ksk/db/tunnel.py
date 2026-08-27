"""cloudflared 실행파일 탐지/설치와, **quant_data 레거시 경로**의 터널 관리.

이 파일에는 소비자가 둘 있고 서로 남남이다:

- `find_cloudflared` / `ensure_cloudflared` — **둘 다** 쓴다. QuantDB 도 `_start_tunnel` 에서
  `ensure_cloudflared()` 를 부른다.
- `manage_db_tunnel` / `kill_tunnel` — **레거시 전용**. download.py / upload.py / tools.py
  (즉 `DBConnection` 진입점)만 쓴다. QuantDB 는 자기 `_start_tunnel`/`_kill_tunnel` 을 쓴다.
"""

import subprocess
import time
import shutil
import os


CLOUDFLARED_INSTALL_HELP = (
    "   1) winget install Cloudflare.cloudflared\n"
    "   2) 직접 다운로드: https://github.com/cloudflare/cloudflared/releases/latest\n"
    "      (Windows 64bit -> cloudflared-windows-amd64.exe)\n"
    "      공식 안내: https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/\n"
    "   받은 exe 는 PATH 에 두거나, .env 의 CLOUDFLARED_BIN 에 전체 경로를 지정하세요."
)


def find_cloudflared():
    """cloudflared.exe의 위치를 찾습니다."""
    path = shutil.which("cloudflared")
    if path:
        return path

    common_paths = [
        r"C:\Program Files (x86)\cloudflared\cloudflared.exe",
        r"C:\Program Files\cloudflared\cloudflared.exe",
        os.path.expanduser(r"~\AppData\Local\Microsoft\WinGet\Packages\Cloudflare.cloudflared_Microsoft.Winget.Source_8wekyb3d8bbwe\cloudflared.exe"),
    ]
    for p in common_paths:
        if os.path.exists(p):
            return p

    return None


def ensure_cloudflared():
    """cloudflared.exe를 찾고, 없으면 winget으로 자동 설치를 시도합니다."""
    cf_exe = find_cloudflared()
    if cf_exe is not None:
        return cf_exe

    print("🔍 cloudflared를 찾을 수 없습니다. 자동 설치를 시도합니다...")
    try:
        subprocess.run(["winget", "install", "Cloudflare.cloudflared", "--silent"], check=False)
    except Exception:
        print("❌ 자동 설치 실패 (winget 을 실행할 수 없습니다). 아래 방법으로 직접 설치하세요:")
        print(CLOUDFLARED_INSTALL_HELP)
        return None

    cf_exe = find_cloudflared()
    if cf_exe is None:
        print("❌ 설치 후에도 파일을 찾을 수 없습니다. 아래 방법으로 직접 설치하세요:")
        print(CLOUDFLARED_INSTALL_HELP)
    return cf_exe


# ── 여기서부터 quant_data 레거시 경로 전용 ──────────────────────────────────
# 아래 둘은 **죽은 코드가 아니다.** download.py / upload.py / tools.py 가 import 시점에
# 끌어다 쓰고, 그 셋은 db/__init__.py 가 무조건 import 한다 — 지우면 `import topquant_ksk.db`
# 자체가 ImportError 로 죽는다(QuantDB 사용자까지 같이).
#
# QuantDB._start_tunnel 과 이름이 비슷하지만 합칠 수 없다. 계약이 다르다:
#   기본 호스트  db.alphawaves.vip        vs  shquantdb.alphawaves.vip (인자로 받음)
#   실행         shell=True + 문자열 cmd   vs  argv 리스트 (셸 미경유)
#   인증         없음                      vs  CF Access 서비스토큰을 env 로 주입
#   대기         고정 1초                  vs  tunnel_wait 인자
#   실패         None 반환(조용히)          vs  RuntimeError + 설치 안내
def manage_db_tunnel(hostname="db.alphawaves.vip", local_port=15432):
    """Cloudflare 보안 터널을 열고 프로세스를 반환합니다 (레거시: DBConnection 경로 전용)."""
    cf_exe = ensure_cloudflared()
    if cf_exe is None:
        return None

    print(f"✅ 실행 파일 확인: {cf_exe}")
    print(f"📡 {hostname} 보안 터널 연결 중 (127.0.0.1:{local_port})...")

    cmd = f'"{cf_exe}" access tcp --hostname {hostname} --url 127.0.0.1:{local_port}'
    try:
        process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True)
        time.sleep(1)
        return process
    except Exception as e:
        print(f"❌ 터널 실행 실패: {e}")
        return None


def kill_tunnel(process):
    """터널 프로세스를 종료합니다 (레거시: manage_db_tunnel 이 돌려준 프로세스용)."""
    if process is None:
        return
    subprocess.run(
        ["taskkill", "/F", "/T", "/PID", str(process.pid)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True,
    )
    print("🔒 터널 종료")
