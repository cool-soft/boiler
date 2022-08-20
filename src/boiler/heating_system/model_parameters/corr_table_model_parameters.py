import pandas as pd

from boiler.constants import column_names
from .abstract_model_requirements import AbstractModelParameters


class CorrTableModelParameters(AbstractModelParameters):

    def __init__(self, timedelta_df: pd.DataFrame) -> None:
        self._timedelta_df = timedelta_df.copy()

    def get_min_heating_system_lag(self) -> pd.Timedelta:
        return self._timedelta_df[column_names.AVG_TIMEDELTA].min()

    def get_max_heating_system_lag(self) -> pd.Timedelta:
        return self._timedelta_df[column_names.AVG_TIMEDELTA].max()
