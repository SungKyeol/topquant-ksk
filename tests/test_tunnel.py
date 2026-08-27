"""cloudflared 실행파일 탐지/자동설치 (`db.tunnel`).

이 두 함수가 QuantDB 접속의 첫 관문이다 — `_start_tunnel` 이 `ensure_cloudflared()` 를 부른다.
여기서 반드시 가로채야 하는 것이 **winget** 이다: 진짜로 부르면 단위테스트가 네트워크를 타고
시스템에 패키지를 깐다. 그래서 아래 테스트는 전부 subprocess.run 을 대체한다.
"""

import subprocess

import topquant_ksk.db.tunnel as tn
from topquant_ksk.db.tunnel import CLOUDFLARED_INSTALL_HELP, ensure_cloudflared, find_cloudflared

_WINGET_CMD = ["winget", "install", "Cloudflare.cloudflared", "--silent"]


class TestFindCloudflared:
    """탐지 순서: PATH -> 알려진 설치경로 -> None. 순서가 뒤집히면 PATH 의 최신본을 두고 옛 exe 를 쓴다."""

    def test_path_hit_wins_and_skips_disk_probe(self, monkeypatch):
        # PATH 에 있으면 디스크를 뒤질 이유가 없다 — exists 를 폭탄으로 깔아 호출 자체를 막는다.
        def _boom(p):
            raise AssertionError("PATH 히트면 알려진 경로를 뒤지면 안 된다")

        monkeypatch.setattr(tn.shutil, "which", lambda name: r"C:\tools\cloudflared.exe")
        monkeypatch.setattr(tn.os.path, "exists", _boom)
        assert find_cloudflared() == r"C:\tools\cloudflared.exe"

    def test_which_receives_bare_name(self, monkeypatch):
        # 'cloudflared.exe' 가 아니라 'cloudflared' 여야 한다 (which 가 PATHEXT 를 붙인다).
        seen = []
        monkeypatch.setattr(tn.shutil, "which", lambda name: seen.append(name) or None)
        monkeypatch.setattr(tn.os.path, "exists", lambda p: False)
        find_cloudflared()
        assert seen == ["cloudflared"]

    def test_common_path_hit_when_not_on_path(self, monkeypatch):
        target = r"C:\Program Files\cloudflared\cloudflared.exe"
        monkeypatch.setattr(tn.shutil, "which", lambda name: None)
        monkeypatch.setattr(tn.os.path, "exists", lambda p: p == target)
        assert find_cloudflared() == target

    def test_winget_package_path_is_probed(self, monkeypatch):
        # winget 자동설치가 떨구는 자리 — 여기를 안 보면 방금 깐 exe 를 못 찾아 설치가 헛돈다.
        monkeypatch.setattr(tn.shutil, "which", lambda name: None)
        probed = []
        monkeypatch.setattr(tn.os.path, "exists", lambda p: probed.append(p) or False)
        find_cloudflared()
        assert any("WinGet" in p and "Cloudflare.cloudflared" in p for p in probed)
        assert not any(p.startswith("~") for p in probed)      # expanduser 로 펴진 절대경로여야 한다

    def test_first_match_wins(self, monkeypatch):
        # 여러 곳에 깔려 있으면 목록 순서대로 첫 번째. (x86 이 앞이다)
        monkeypatch.setattr(tn.shutil, "which", lambda name: None)
        monkeypatch.setattr(tn.os.path, "exists", lambda p: "cloudflared" in p)
        assert find_cloudflared() == r"C:\Program Files (x86)\cloudflared\cloudflared.exe"

    def test_returns_none_when_nowhere(self, monkeypatch):
        monkeypatch.setattr(tn.shutil, "which", lambda name: None)
        monkeypatch.setattr(tn.os.path, "exists", lambda p: False)
        assert find_cloudflared() is None


class TestEnsureCloudflared:
    """없으면 winget 으로 깔아 본다. 설치를 시도한 뒤에는 **반드시 다시 탐지**해야 한다."""

    @staticmethod
    def _finds(monkeypatch, *results):
        """find_cloudflared 를 호출 순서대로 정해진 값을 뱉도록 바꾸고, 호출 로그를 돌려준다."""
        calls = []

        def fake():
            calls.append(len(calls))
            return results[min(len(calls) - 1, len(results) - 1)]

        monkeypatch.setattr(tn, "find_cloudflared", fake)
        return calls

    def test_already_installed_skips_winget(self, monkeypatch, capsys):
        # 이미 있으면 winget 을 부르면 안 된다 — 매 접속마다 설치를 시도하면 접속이 몇 초씩 늦어진다.
        self._finds(monkeypatch, r"C:\tools\cloudflared.exe")
        monkeypatch.setattr(tn.subprocess, "run",
                            lambda *a, **k: (_ for _ in ()).throw(AssertionError("winget 금지")))
        assert ensure_cloudflared() == r"C:\tools\cloudflared.exe"
        assert capsys.readouterr().out == ""                   # 조용히 통과

    def test_winget_success_returns_freshly_found_path(self, monkeypatch):
        # 설치 전 None -> winget -> 설치 후 경로. 두 번째 탐지 결과를 돌려줘야 한다.
        calls = self._finds(monkeypatch, None, r"C:\winget\cloudflared.exe")
        seen = {}
        monkeypatch.setattr(tn.subprocess, "run",
                            lambda cmd, **kw: seen.update(cmd=cmd, kw=kw))
        assert ensure_cloudflared() == r"C:\winget\cloudflared.exe"
        assert len(calls) == 2                                  # 설치 후 재탐지했다
        assert seen["cmd"] == _WINGET_CMD                       # 무인 설치 (--silent) + 정확한 패키지 id
        assert seen["kw"].get("check") is False                 # winget 비정상종료로 죽지 않는다

    def test_winget_unavailable_prints_manual_help(self, monkeypatch, capsys):
        # winget 자체가 없는 PC (FileNotFoundError) — 여기서 죽으면 사용자는 원인을 못 본다.
        calls = self._finds(monkeypatch, None)

        def _no_winget(cmd, **kw):
            raise FileNotFoundError("winget")

        monkeypatch.setattr(tn.subprocess, "run", _no_winget)
        assert ensure_cloudflared() is None
        assert len(calls) == 1                                  # 설치가 아예 안 됐으니 재탐지도 없다
        out = capsys.readouterr().out
        assert "자동 설치 실패" in out
        assert CLOUDFLARED_INSTALL_HELP in out                  # 수동 설치 안내를 그대로 보여준다
        assert "CLOUDFLARED_BIN" in out                         # 마지막 탈출구(경로 직접 지정)까지

    def test_winget_ran_but_still_missing_prints_help(self, monkeypatch, capsys):
        # winget 이 조용히 실패하는 경우(check=False 라 예외가 아니다) — 설치 후에도 못 찾으면 안내.
        calls = self._finds(monkeypatch, None, None)
        monkeypatch.setattr(tn.subprocess, "run", lambda cmd, **kw: subprocess.CompletedProcess(cmd, 1))
        assert ensure_cloudflared() is None
        assert len(calls) == 2                                  # 재탐지는 했고
        out = capsys.readouterr().out
        assert "설치 후에도" in out
        assert CLOUDFLARED_INSTALL_HELP in out
