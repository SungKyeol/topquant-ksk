# CLAUDE.md

## 프로젝트 개요
- 패키지명: `topquant-ksk`
- 구조: src layout (`src/topquant_ksk/`)
- 서브패키지: `db` (tunnel, upload, download, tools)
- 선택적 의존성: `[db]`, `[plot]`, `[all]`

## PyPI 배포 절차
모든 명령어는 `powershell -Command` 래퍼로 실행.

```
1. dist 정리:  Remove-Item -Path "c:\Users\SungKyeol\Desktop\github\topquant-ksk\dist\*" -Force
2. 빌드:      & "C:\ProgramData\anaconda3\python.exe" -m build "c:\Users\SungKyeol\Desktop\github\topquant-ksk"
3. 업로드:    & "C:\ProgramData\anaconda3\python.exe" -m twine upload --disable-progress-bar "c:\Users\SungKyeol\Desktop\github\topquant-ksk\dist\*"
```

- Python: `C:\ProgramData\anaconda3\python.exe`
- `.pypirc`: `C:\Users\SungKyeol\.pypirc` (자동 인증)
- twine에 `--disable-progress-bar` 필수 (cp949 + rich 충돌 방지)
- 에디터블 모드로 개발 중 → `pip install topquant-ksk` 실행 시 에디터블 연결 끊김 주의

## 코드 변경은 worktree 에서 — 메인 워킹트리를 직접 고치지 마라

이 리포는 **여러 Claude 세션이 동시에 붙는다.** 실제로 2026-08-27 에 두 세션이 같은
`C:/sungkyeol/topquant-ksk` 를 동시에 편집해, 한쪽이 `quantdb.py` 를 패치하는 사이
다른 쪽이 같은 함수에 캐시 계층을 넣었다. 그때 드러난 비용은 셋이다:
- 앵커가 어긋나 패치 스크립트가 중간에 죽는다 (`warnings.warn` → `_warn` 으로 바뀐 뒤).
- 커밋이 남의 미완성 작업을 통째로 삼킨다 — 무엇이 누구 것인지 커밋에서 안 보인다.
- 같은 문제를 각자 고쳐 놓고 머지에서 add/add 충돌로 만난다 (`tests/test_tunnel.py`).

**그래서 파일을 건드리기 전에 worktree 를 판다.**

```bash
git worktree add .worktrees/<주제> -b <브랜치명>
```

- 토론·조사·읽기 전용 확인은 메인 트리에서 해도 된다. **첫 편집** 전에 worktree 가
  이미 있어야 한다.
- 작업이 끝나면 PR/머지 후 `worktree-clear-topquant` 스킬로 정리한다 — 윈도우에서는
  코덱스 리뷰 서버가 디렉터리를 잡고 있어 `git worktree remove` 가 껍데기를 남긴다.
- 메인 트리에서 이미 편집을 시작했다면, 커밋 전에 `ListAgents` 로 피어 세션을 확인하고
  같은 파일을 만지는 세션이 있으면 먼저 말을 걸어라.

## 코딩 규칙
- import 문은 항상 파일 맨 위에 작성. 함수 내부 import 금지.
