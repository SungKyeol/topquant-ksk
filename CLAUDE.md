# CLAUDE.md

## 프로젝트 개요
- 패키지명: `topquant-ksk`
- 구조: src layout (`src/topquant_ksk/`)
- 서브패키지: `db` (tunnel, upload, download, tools)
- 선택적 의존성: `[db]`, `[plot]`, `[all]`

## PyPI 배포 절차

리포 루트(`C:\sungkyeol\topquant-ksk`)에서, bash 로. 0.3.0 배포 때 실제로 이 순서로 나갔다
(2026-08-27 실측).

```bash
# 1. pyproject.toml 의 version 을 올리고 `chore(release): X.Y.Z` 로 커밋 + main push
rm -rf dist/*                                 # 2. dist 정리
python -m build                               # 3. 빌드 -> dist/*.whl, *.tar.gz
python -m twine check dist/*                  # 4. 검사 (long_description 경고는 알려진 것)
                                              # 5. 업로드
PYTHONUTF8=1 python -m twine upload --disable-progress-bar C:/sungkyeol/topquant-ksk/dist/*
```

**`PYTHONUTF8=1` 이 없으면 5번이 죽는다.** twine 이 `.pypirc` 를 열 때 인코딩을 지정하지
않아(`twine/utils.py` 의 `with open(realpath) as f:`) 한국어 Windows 로케일인 cp949 로
디코딩하는데, 이 PC 의 `.pypirc` 는 **한글 주석**이 든 UTF-8 파일이다:

```
UnicodeDecodeError: 'cp949' codec can't decode byte 0xec in position 7
```

자격증명 자체는 ASCII 라 멀쩡하다 — 죽는 건 주석 줄이다. UTF-8 모드로 돌리면 `open()` 기본
인코딩이 UTF-8 이 되어 통과한다. 영구적으로 없애려면 `.pypirc` 의 한글 주석을 ASCII 로 바꾼다.

- Python: `python` (= `C:\Users\AI_Quant\anaconda3\python.exe`). **`C:\ProgramData\anaconda3`
  는 이 PC 에 없다** — 옛 문서가 그 경로를 적어 두었으나 실재하지 않는다.
- `.pypirc`: `C:\Users\AI_Quant\.pypirc` (`[pypi]` + 토큰, 자동 인증)
- `--disable-progress-bar` 는 유지한다 (cp949 + rich 충돌 방지)
- 에디터블 모드로 개발 중 → `pip install topquant-ksk` 실행 시 에디터블 연결 끊김 주의

### worktree 에서 테스트할 때

에디터블 설치가 **메인 트리**(`C:/sungkyeol/topquant-ksk/src`)를 가리키므로, worktree 에서
그냥 `pytest` 를 돌리면 **worktree 가 아니라 메인 트리 코드를 테스트한다.** 반드시:

```bash
PYTHONPATH="C:/sungkyeol/topquant-ksk/.worktrees/<이름>/src" python -m pytest tests/
```

(2026-08-27 실측: 이걸 모르고 "176 passed" 를 보고 있었는데 새 코드는 한 줄도 안 타고 있었다.)

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
