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


def manage_db_tunnel(hostname="db.alphawaves.vip", local_port=15432):
    """Cloudflare 보안 터널을 열고 프로세스를 반환합니다."""
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
    """터널 프로세스를 종료합니다."""
    if process is None:
        return
    subprocess.run(
        ["taskkill", "/F", "/T", "/PID", str(process.pid)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True,
    )
    print("🔒 터널 종료")
