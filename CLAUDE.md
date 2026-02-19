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
