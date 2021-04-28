import logging
from typing import List, Optional

import pandas as pd

from boiler.constants import column_names
from boiler.dataset_processing.dataset_processor import DatasetProcessor


# TODO: вынести интерполятор в алгоритмы
class DatasetInterpolator(DatasetProcessor):

    def __init__(self,
                 interpolation_step: pd.Timedelta = pd.Timedelta(seconds=180),
                 columns_to_interpolate: Optional[List[str]] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        if columns_to_interpolate is None:
            columns_to_interpolate = []
        self._columns_to_interpolate = columns_to_interpolate
        self._interpolation_step = interpolation_step

        self._logger.debug(f"Interpolation step is {interpolation_step}")
        self._logger.debug(f"Columns to interpolate {columns_to_interpolate}")

    def set_columns_to_interpolate(self, columns: List[str]) -> None:
        self._columns_to_interpolate = columns

    def set_interpolation_step(self, interpolation_step: pd.Timedelta) -> None:
        self._interpolation_step = interpolation_step

    def process_df(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.debug("Interpolating is requested")
        df = df.copy()
        df.sort_values(by=column_names.TIMESTAMP, ignore_index=True, inplace=True)
        df = self._interpolate_passes_of_datetime(df)
        self._interpolate_passes_of_data(df)
        self._logger.debug("Interpolated")
        return df

    def _interpolate_passes_of_datetime(self, df: pd.DataFrame):
        self._logger.debug("Interpolating passes of datetime")

        datetime_to_insert = []
        previous_datetime = None
        for timestamp in df[column_names.TIMESTAMP].to_list():
            if previous_datetime is None:
                previous_datetime = timestamp
                continue
            next_datetime = timestamp

            current_datetime = previous_datetime + self._interpolation_step
            while current_datetime < next_datetime:
                datetime_to_insert.append({
                    column_names.TIMESTAMP: current_datetime
                })
                current_datetime += self._interpolation_step

            previous_datetime = next_datetime

        df = df.append(datetime_to_insert, ignore_index=True)
        df.sort_values(by=column_names.TIMESTAMP, ignore_index=True, inplace=True)

        return df

    def _interpolate_passes_of_data(self, df: pd.DataFrame) -> None:
        self._logger.debug("Interpolating passes of data")
        for column_to_interpolate in self._columns_to_interpolate:
            df[column_to_interpolate] = pd.to_numeric(df[column_to_interpolate], downcast="float")
            df[column_to_interpolate].interpolate(inplace=True)
