# 시각화

## heatmap

DataFrame을 RdYlBu_r 컬러맵 히트맵으로 시각화합니다. 값은 자동으로 % 변환됩니다.

```python
from topquant_ksk import heatmap

heatmap(
    dataframe,
    size=(12, 6),
    annot=True,
    vmax=None,
    vmin=None,
    title=None,
    rotation=0,
    fontsize=25,
    show_colorbar=False,
)
```

| 파라미터 | 타입 | 기본값 | 설명 |
|----------|------|--------|------|
| `dataframe` | DataFrame | - | 히트맵 데이터 |
| `size` | tuple | `(12, 6)` | 그림 크기 |
| `annot` | bool | `True` | 셀 값 표시 여부 |
| `vmax` | float \| None | `None` | 컬러맵 최대값 |
| `vmin` | float \| None | `None` | 컬러맵 최소값 |
| `title` | str \| None | `None` | 차트 제목 |
| `rotation` | int | `0` | x축 레이블 회전 각도 |
| `fontsize` | int | `25` | 폰트 크기 |
| `show_colorbar` | bool | `False` | 컬러바 표시 여부 |
