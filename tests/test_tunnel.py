"""topquant_ksk.db.tunnel 단위 테스트.

cloudflared 탐지(find_cloudflared)와 winget 자동 설치 경로(ensure_cloudflared)를
실제 파일시스템/winget 없이 검증한다.
"""
import os
from types import SimpleNamespace

import pytest

import topquant_ksk.db.tunnel as tn
from topquant_ksk.db.tunnel import (
    CLOUDFLARED_INSTALL_HELP,
    ensure_cloudflared,
    find_cloudflared,
)

_PROGRAM_FILES_X86 = r"C:\Program Files (x86)\cloudflared\cloudflared.exe"
_WINGET_ARGV = ["winget", "install", "Cloudflare.cloudflared", "--silent"]


def _fake_os(exists):
    """tn 모듈이 보는 os 만 갈아끼운다 (전역 os.path.exists 오염 방지)."""
    return SimpleNamespace(path=SimpleNamespace(exists=exists, expanduser=os.path.expanduser))


class TestFindCloudflared:
    def test_which_hit_short_circuits_common_paths(self, monkeypatch):
        probed = []
        monkeypatch.setattr(tn.shutil, "which", lambda name: r"C:\bin\cloudflared.exe")
        monkeypatch.setattr(tn, "os", _fake_os(lambda p: probed.append(p) or False))

        assert find_cloudflared() == r"C:\bin\cloudflared.exe"
        assert probed == []                       # PATH 에서 찾았으면 후보 경로는 안 뒤진다

    def test_falls_back_to_common_install_path(self, monkeypatch):
        monkeypatch.setattr(tn.shutil, "which", lambda name: None)
        monkeypatch.setattr(tn, "os", _fake_os(lambda p: p == _PROGRAM_FILES_X86))

        assert find_cloudflared() == _PROGRAM_FILES_X86

    def test_probes_winget_package_dir(self, monkeypatch):
        winget_exe = os.path.expanduser(
            r"~\AppData\Local\Microsoft\WinGet\Packages"
            r"\Cloudflare.cloudflared_Microsoft.Winget.Source_8wekyb3d8bbwe\cloudflared.exe"
        )
        monkeypatch.setattr(tn.shutil, "which", lambda name: None)
        monkeypatch.setattr(tn, "os", _fake_os(lambda p: p == winget_exe))

        assert find_cloudflared() == winget_exe

    def test_returns_none_when_nothing_found(self, monkeypatch):
        monkeypatch.setattr(tn.shutil, "which", lambda name: None)
        monkeypatch.setattr(tn, "os", _fake_os(lambda p: False))

        assert find_cloudflared() is None


class TestEnsureCloudflared:
    def test_returns_existing_binary_without_winget(self, monkeypatch, capsys):
        runs = []
        monkeypatch.setattr(tn, "find_cloudflared", lambda: r"C:\bin\cloudflared.exe")
        monkeypatch.setattr(tn.subprocess, "run", lambda cmd, **kw: runs.append(cmd))

        assert ensure_cloudflared() == r"C:\bin\cloudflared.exe"
        assert runs == []                         # 이미 있으면 winget 을 부르지 않는다
        assert capsys.readouterr().out == ""

    def test_installs_via_winget_then_returns_new_path(self, monkeypatch, capsys):
        results = iter([None, r"C:\winget\cloudflared.exe"])
        captured = {}

        def fake_run(cmd, **kw):
            captured["cmd"] = cmd
            captured["kw"] = kw
            return SimpleNamespace(returncode=0)

        monkeypatch.setattr(tn, "find_cloudflared", lambda: next(results))
        monkeypatch.setattr(tn.subprocess, "run", fake_run)

        assert ensure_cloudflared() == r"C:\winget\cloudflared.exe"
        assert captured["cmd"] == _WINGET_ARGV
        assert captured["kw"] == {"check": False}
        assert CLOUDFLARED_INSTALL_HELP not in capsys.readouterr().out

    def test_winget_unavailable_returns_none_with_help(self, monkeypatch, capsys):
        def boom(cmd, **kw):
            raise FileNotFoundError("winget")

        monkeypatch.setattr(tn, "find_cloudflared", lambda: None)
        monkeypatch.setattr(tn.subprocess, "run", boom)

        assert ensure_cloudflared() is None
        assert CLOUDFLARED_INSTALL_HELP in capsys.readouterr().out

    def test_still_missing_after_install_returns_none_with_help(self, monkeypatch, capsys):
        calls = []
        monkeypatch.setattr(tn, "find_cloudflared", lambda: None)
        monkeypatch.setattr(tn.subprocess, "run",
                            lambda cmd, **kw: calls.append(cmd) or SimpleNamespace(returncode=0))

        assert ensure_cloudflared() is None
        assert calls == [_WINGET_ARGV]            # 설치는 시도했다
        assert CLOUDFLARED_INSTALL_HELP in capsys.readouterr().out
