import os
import pandas as pd
import warnings

# ==============================================================================
# ## 헬퍼(Helper) 함수 (이전과 동일)
# ==============================================================================

def find_file_recursive(filename: str) -> str | None:
    """현재 디렉토리와 하위에서 파일을 재귀적으로 찾아 경로를 반환합니다."""
    root_dir = os.getcwd()
    for root, _, files in os.walk(root_dir):
        if filename in files:
            return os.path.join(root, filename)
    return None

def _load_file(file_path: str, sheet_name: str | None = None, encoding: str = 'utf-8') -> pd.DataFrame | None:
    """파일 경로와 시트 이름(선택)으로 데이터프레임을 로드하는 공통 함수"""
    try:
        if file_path.endswith('.xlsx'):
            return pd.read_excel(file_path, sheet_name=sheet_name, index_col=[0])
        elif file_path.endswith('.csv'):
            try:
                return pd.read_csv(file_path, encoding=encoding, index_col=[0], low_memory=False)
            except UnicodeDecodeError:
                print(f"UTF-8 load Error, trying CP949...")
                return pd.read_csv(file_path, encoding='cp949', index_col=[0], low_memory=False)
        else:
            print(f"지원하지 않는 파일 형식입니다: {file_path}")
            return None
    except Exception as e:
        print(f"파일 로드 중 오류: {e}")
        return None

def _process_dataframe(df: pd.DataFrame, dropna_cols: bool = True) -> pd.DataFrame:
    """데이터프레임 후처리를 위한 공통 함수"""
    idx = df.index

    # Excel 시리얼 넘버를 날짜로 변환 시도
    numeric_idx = pd.to_numeric(idx, errors='coerce')
    excel_dates = pd.to_datetime(numeric_idx, unit='D', origin='1899-12-30', errors='coerce')

    # 문자열 날짜 형식도 시도
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        string_dates = pd.to_datetime(idx, errors='coerce')

    # 둘 중 하나라도 유효한 날짜면 사용 (Series로 변환하여 fillna 수행)
    excel_series = pd.Series(excel_dates, index=idx)
    string_series = pd.Series(string_dates, index=idx)
    parsed_dates = excel_series.fillna(string_series)

    # 유효한 날짜가 1개라도 있는지 확인
    has_valid_dates = parsed_dates.notna().any()

    if has_valid_dates:
        # 유효한 날짜가 1개라도 있으면 날짜 인덱스 사용 (기존 로직)
        non_date_elements = idx[parsed_dates.isna()]
        df.drop(non_date_elements, inplace=True)
        df.index = pd.DatetimeIndex(parsed_dates.dropna())
    else:
        # 유효한 날짜가 하나도 없으면 기본 인덱싱 사용
        df = df.reset_index(drop=True)

    df.index.name = None
    df.replace(',', '', regex=True, inplace=True)
    if dropna_cols:
        df.dropna(how='all', axis=1, inplace=True)
    print("float 타입으로 변환을 시도합니다...")
    def keep_string_convert(series):
        return pd.to_numeric(series, errors='ignore')
    df_converted = df.apply(keep_string_convert, axis=0)

    # 에러 컬럼 제거 (레벨 개수-1 이상이 에러인 컬럼)
    error_values = {'#VALUE!', '#N/A', '#N/A N/A'}
    if isinstance(df_converted.columns, pd.MultiIndex):
        col_df = pd.DataFrame(df_converted.columns.tolist())
        n_levels = col_df.shape[1]
        # 각 컬럼에서 에러 값 또는 NaN인 레벨 개수 계산
        error_count = (col_df.isin(error_values) | col_df.isna()).sum(axis=1)
        # 에러 개수가 (레벨 개수 - 1) 이상이면 drop
        error_mask = ~(error_count >= n_levels - 1).values
    else:
        error_mask = ~df_converted.columns.isin(error_values)
    df_converted = df_converted.loc[:, error_mask]

    return df_converted

# ✨ 수정된 마스터 헬퍼 함수
def _load_and_process_data(filename: str, column_spec: list, data_type_name: str, sheet_name: str | None = None, encoding: str = 'utf-8', dropna_cols: bool = True) -> pd.DataFrame | None:
    """파일 검색, 로드, 후처리 전체 과정을 수행하는 마스터 헬퍼 함수"""
    file_path = find_file_recursive(filename)
    if not file_path:
        print(f"'{filename}' 파일을 찾을 수 없습니다. 🤷‍♂️")
        return None

    print(f"파일 발견! '{file_path}' 파일을 로드합니다... 📂")
    df = _load_file(file_path, sheet_name=sheet_name, encoding=encoding)
    if df is None:
        return None

    print(f"{data_type_name} 데이터 후처리를 시작합니다... 🛠️")
    
    # --- ✨ 컬럼 설정 로직 ✨ ---
    if len(column_spec) == 1:
        df.columns = df.loc[column_spec[0]]
    else:
        df.columns = pd.MultiIndex.from_arrays([df.loc[name] for name in column_spec])

    # column_spec 행들 무조건 drop
    df.drop(column_spec, inplace=True, errors='ignore')

    # 유효 데이터가 1개 이하인 행 drop
    df = df[df.count(axis=1) > 1]

    # 공통 후처리 로직 호출
    df = _process_dataframe(df, dropna_cols=dropna_cols)
    
    print("처리 완료! ✨")
    return df

# ==============================================================================
# ## ✨ 메인(Main) 데이터 로드 함수 (수정됨) ✨
# ==============================================================================

def load_FactSet_TimeSeriesData(
    filename: str,
    column_spec: list,
    sheet_name: str | None = 'TimeSeries',
    encoding: str = 'utf-8',
    dropna_cols: bool = False
) -> pd.DataFrame | None:
    """TimeSeries 데이터를 로드합니다. """

    # CSV 파일이면 sheet_name을 None으로 자동 설정
    if filename.endswith('.csv'):
        sheet_name = None

    return _load_and_process_data(
        filename=filename,
        sheet_name=sheet_name,
        column_spec=column_spec,
        data_type_name='TimeSeries',
        encoding=encoding,
        dropna_cols=dropna_cols
    )


def load_DataGuide_TimeSeriesData(
    filename: str,
    sheet_name: str | None = 'TimeSeries',
    column_spec: list | None = None,
    dropna_cols: bool = True,
    encoding: str = 'utf-8'
) -> pd.DataFrame | None:
    """TimeSeries 데이터를 로드합니다. (3-level columns)"""
    if column_spec is None:
        column_spec = ['Item Name', 'Symbol Name', 'Symbol']

    return _load_and_process_data(
        filename=filename,
        sheet_name=sheet_name,
        column_spec=column_spec,
        data_type_name='TimeSeries',
        dropna_cols=dropna_cols,
        encoding=encoding
    )

def load_DataGuide_IndexData(
    filename: str,
    sheet_name: str | None = 'TimeSeries',
    column_spec: list | None = None,
    dropna_cols: bool = True,
    encoding: str = 'utf-8'
) -> pd.DataFrame | None:
    """Index 데이터를 로드합니다. (2-level columns)"""
    if column_spec is None:
        column_spec = ['Item Name', 'Symbol Name']

    return _load_and_process_data(
        filename=filename,
        sheet_name=sheet_name,
        column_spec=column_spec,
        data_type_name='Index',
        dropna_cols=dropna_cols,
        encoding=encoding
    )

def load_DataGuide_EconomicData(
    filename: str,
    sheet_name: str | None = 'Economic',
    column_spec: list | None = None,
    dropna_cols: bool = True,
    encoding: str = 'utf-8'
) -> pd.DataFrame | None:
    """Economic 데이터를 로드합니다. (1-level column)"""
    if column_spec is None:
        column_spec = ['Item Name']

    return _load_and_process_data(
        filename=filename,
        sheet_name=sheet_name,
        column_spec=column_spec,
        data_type_name='Economic',
        dropna_cols=dropna_cols,
        encoding=encoding
    )

def load_DataGuide_CrossSectionalData(
    filename: str,
    encoding: str = 'utf-8'
) -> pd.DataFrame | None:
    """
    지정된 파일명으로 CrossSectional 데이터를 찾아 로드하고 전처리합니다.
    """
    file_path = find_file_recursive(filename)

    if not file_path:
        print(f"현재 폴더 및 하위 폴더에서 '{filename}' 파일을 찾을 수 없습니다. 🤷‍♂️")
        return None

    print(f"파일 발견! '{file_path}' 파일을 로드합니다... 📂")

    try:
        if file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path, sheet_name='CrossSectional', index_col=[1, 0])
        elif file_path.endswith('.csv'):
            try:
                df = pd.read_csv(file_path, encoding=encoding, index_col=[1, 0], low_memory=False)
            except UnicodeDecodeError:
                print(f"UTF-8 load Error, trying CP949...")
                df = pd.read_csv(file_path, encoding='cp949', index_col=[1, 0], low_memory=False)
        else:
            print(f"지원하지 않는 파일 형식입니다: {filename}")
            return None
    except Exception as e:
        print(f"파일 로드 중 오류: {e}")
        return None

    print("CrossSectional 데이터 후처리를 시작합니다... 🛠️")
    header_tuple = ('Name', 'Symbol')
    df.columns = df.loc[header_tuple]
    header_location = df.index.get_loc(header_tuple)
    df = df.iloc[header_location + 1:]
    df.columns.names = ['Item Name']
    df.index.names = ['Name', 'Symbol']
    df.replace(',', '', regex=True, inplace=True)

    print("float 타입으로 변환을 시도합니다...")
    for col in df.columns:
        try:
            df[col] = df[col].astype(float)
        except ValueError:
            pass

    print("처리 완료! ✨")
    return df