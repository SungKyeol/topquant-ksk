# 데이터 로딩

FactSet, DataGuide 등 금융 데이터 소스의 Excel/CSV 파일을 pandas MultiIndex DataFrame으로 로딩합니다.

## load_FactSet_TimeSeriesData

FactSet 시계열 데이터 (Excel/CSV)를 로딩합니다.

```python
from topquant_ksk import load_FactSet_TimeSeriesData

df = load_FactSet_TimeSeriesData(
    filename="data.xlsx",
    column_spec=['ticker', 'item_name'],  # MultiIndex 레벨
    sheet_name='TimeSeries',
    encoding='utf-8',
    dropna_cols=False,
    type_conversion='float',  # 'float', 'str', None
)
```

| 파라미터 | 타입 | 기본값 | 설명 |
|----------|------|--------|------|
| `filename` | str | - | 파일 경로 |
| `column_spec` | list | - | MultiIndex 레벨로 사용할 컬럼명 리스트 |
| `sheet_name` | str | `'TimeSeries'` | 시트명 |
| `encoding` | str | `'utf-8'` | 인코딩 |
| `dropna_cols` | bool | `False` | 전체 NaN 컬럼 제거 여부 |
| `type_conversion` | str \| None | `'float'` | 타입 변환 (`'float'`, `'str'`, `None`) |

---

## load_DataGuide_TimeSeriesData

DataGuide 시계열 데이터를 로딩합니다 (3레벨 MultiIndex).

```python
from topquant_ksk import load_DataGuide_TimeSeriesData

df = load_DataGuide_TimeSeriesData(
    filename="dg_data.xlsx",
    column_spec=['Item Name', 'Symbol Name', 'Symbol'],
)
```

---

## 기타 DataGuide 로딩 함수

```python
from topquant_ksk import (
    load_DataGuide_IndexData,
    load_DataGuide_EconomicData,
    load_DataGuide_CrossSectionalData,
)

# 인덱스 데이터 (2레벨 MultiIndex)
load_DataGuide_IndexData(filename, column_spec=['Item Name', 'Symbol Name'])

# 경제 데이터 (1레벨)
load_DataGuide_EconomicData(filename, column_spec=['Item Name'])

# 횡단면 데이터
load_DataGuide_CrossSectionalData(filename, encoding='utf-8')
```
