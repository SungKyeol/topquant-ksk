import pandas as pd
from . import download, upload, tools


class _Download:
    def __init__(self, db_user, db_password, local_host):
        self._u = db_user
        self._p = db_password
        self._l = local_host

    def fetch_timeseries_table(self, table_name: str, columns: list = None, item_names: list = None, limit: int = None, start_date: str | int = None, end_date: str | int = None, sedols: list | str = "all", etf_ticker: list | str | None = None, save_and_reload_pickle_cache: bool = False) -> pd.DataFrame:
        return download.fetch_timeseries_table(table_name=table_name, columns=columns, item_names=item_names, limit=limit, start_date=start_date, end_date=end_date, sedols=sedols, etf_ticker=etf_ticker, save_and_reload_pickle_cache=save_and_reload_pickle_cache, db_user=self._u, db_password=self._p, local_host=self._l)

    def fetch_master_table(self, columns: list, table_name: str = "public.master_table") -> pd.DataFrame:
        return download.fetch_master_table(columns=columns, table_name=table_name, db_user=self._u, db_password=self._p, local_host=self._l)

    def fetch_universe_mask(self, etf_ticker: str | list, table_name: str = "public.monthly_etf_constituents") -> pd.DataFrame:
        return download.fetch_universe_mask(etf_ticker=etf_ticker, table_name=table_name, db_user=self._u, db_password=self._p, local_host=self._l)



class _Upload:
    def __init__(self, db_user, db_password, local_host):
        self._u = db_user
        self._p = db_password
        self._l = local_host

    def upload_index_DataFrame_with_polars(self, df: pd.DataFrame, table_name: str = "adjusted_time_series_data_index", truncate: bool = False) -> pd.DataFrame:
        return upload.upload_index_DataFrame_with_polars(df=df, table_name=table_name, truncate=truncate, db_user=self._u, db_password=self._p, local_host=self._l)

    def upload_index_macro_DataFrame_with_polars(self, df: pd.DataFrame, col_map: dict, table_name: str, truncate: bool = False) -> pd.DataFrame:
        return upload.upload_index_macro_DataFrame_with_polars(df=df, col_map=col_map, table_name=table_name, truncate=truncate, db_user=self._u, db_password=self._p, local_host=self._l)

    def upload_stock_timeseries_DataFrame_with_polars(self, dfs: list, value_names: list, table_name: str, truncate: bool = False) -> pd.DataFrame:
        return upload.upload_stock_timeseries_DataFrame_with_polars(dfs=dfs, value_names=value_names, table_name=table_name, truncate=truncate, db_user=self._u, db_password=self._p, local_host=self._l)

    def upload_static_variables_DataFrame_with_polars(self, df: pd.DataFrame, column_names: list = ['ticker', 'company_name', 'sedol'], value_column_map: dict = {'P_DCOUNTRY': 'primary_domicile_of_country'}, table_name: str = "public.master_table", truncate: bool = False) -> None:
        return upload.upload_static_variables_DataFrame_with_polars(df=df, column_names=column_names, value_column_map=value_column_map, table_name=table_name, truncate=truncate, db_user=self._u, db_password=self._p, local_host=self._l)

    def upload_latest_level_with_polars(self, df: pd.DataFrame, table_name: str = "public.adj_latest_level_stock", truncate: bool = True, conflict_keys: list = ['sedol', 'item_name']) -> None:
        return upload.upload_latest_level_with_polars(df=df, table_name=table_name, truncate=truncate, conflict_keys=conflict_keys, db_user=self._u, db_password=self._p, local_host=self._l)

    def upload_etf_constituents_DataFrame_with_polars(self, dfs: list, universe_names: list, table_name: str = "public.monthly_etf_constituents") -> pd.DataFrame:
        return upload.upload_etf_constituents_DataFrame_with_polars(dfs=dfs, universe_names=universe_names, table_name=table_name, db_user=self._u, db_password=self._p, local_host=self._l)

    def refresh_materialized_view_concurrently(self, table_name: str, source_tables: list[str] = None, join_keys: list[str] = None, unique_index_cols: list[str] = None) -> None:
        return upload.refresh_materialized_view_concurrently(table_name=table_name, source_tables=source_tables, join_keys=join_keys, unique_index_cols=unique_index_cols, db_user=self._u, db_password=self._p, local_host=self._l)


class _Tools:
    def __init__(self, db_user, db_password, local_host):
        self._u = db_user
        self._p = db_password
        self._l = local_host

    def check_existing_tables(self, detailed_column_date: bool = True) -> None:
        return tools.check_existing_tables(detailed_column_date=detailed_column_date, db_user=self._u, db_password=self._p, local_host=self._l)


class DBConnection:
    def __init__(self, db_user: str, db_password: str, local_host: bool = False):
        self.download = _Download(db_user, db_password, local_host)
        self.upload = _Upload(db_user, db_password, local_host)
        self.tools = _Tools(db_user, db_password, local_host)
