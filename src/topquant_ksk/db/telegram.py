"""
FactSet DB 파이프라인 텔레그램 알림 유틸리티
"""
import urllib.request
import json
import traceback
from datetime import datetime


def send_telegram(message: str, bot_token: str, chat_id: str) -> None:
    """
    텔레그램 메시지를 전송합니다.
    전송 실패 시 예외를 삼키고 stderr에만 출력합니다.
    (알림 실패가 데이터 파이프라인을 중단시키면 안 됨)
    """
    if not bot_token or not chat_id:
        print("[telegram] TELEGRAM_DB_BOT_TOKEN 또는 TELEGRAM_DB_CHAT_ID가 설정되지 않았습니다.")
        return

    # 텔레그램 메시지 길이 제한 (4096자)
    if len(message) > 4096:
        message = message[:4090] + "\n...(잘림)"

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = json.dumps({
        "chat_id": chat_id,
        "text": message,
    }).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            pass
    except Exception as e:
        print(f"[telegram] 텔레그램 전송 실패: {e}")


def notify_success(module_name: str, bot_token: str, chat_id: str, detail: str = "") -> None:
    """작업 완료 알림"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = (
        f"[DB 업데이트 완료]\n"
        f"모듈: {module_name}\n"
        f"시간: {now}\n"
        f"상태: 정상 완료"
    )
    if detail:
        msg += f"\n비고: {detail}"
    send_telegram(msg, bot_token, chat_id)


def notify_error(module_name: str, exception: BaseException, bot_token: str, chat_id: str) -> None:
    """에러 발생 알림 (traceback 포함)"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tb = traceback.format_exc()
    # traceback 길이 제한
    if len(tb) > 2000:
        tb = tb[:500] + "\n...(중략)...\n" + tb[-1500:]
    msg = (
        f"[DB 업데이트 실패]\n"
        f"모듈: {module_name}\n"
        f"시간: {now}\n"
        f"에러: {type(exception).__name__}: {exception}\n"
        f"\n{tb}"
    )
    send_telegram(msg, bot_token, chat_id)
