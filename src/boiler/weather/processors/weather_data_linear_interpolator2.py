import logging
from datetime import timedelta
from typing import List, Optional

import pandas as pd

from boiler.constants import column_names
from .weather_data_processor import WeatherDataProcessor


class WeatherDataLinearInterpolator(WeatherDataProcessor):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance of the provider")

        self._columns_to_interpolate = (
            column_names.FORWARD_PIPE_COOLANT_TEMP,
            column_names.BACKWARD_PIPE_COOLANT_TEMP,
            column_names.FORWARD_PIPE_COOLANT_VOLUME,
            column_names.BACKWARD_PIPE_COOLANT_VOLUME,
            column_names.FORWARD_PIPE_COOLANT_PRESSURE,
            column_names.BACKWARD_PIPE_COOLANT_PRESSURE
        )
        self._interpolation_step = timedelta(seconds=180)

    def set_columns_to_interpolate(self, columns: List[str]) -> None:
        self._columns_to_interpolate = columns

    def set_interpolation_step(self, interpolation_step: pd.Timedelta) -> None:
        self._interpolation_step = interpolation_step

    def process_weather_df(
            self,
            df: pd.DataFrame,
            start_datetime: Optional[pd.Timestamp] = None,
            end_datetime: Optional[pd.Timestamp] = None,
            inplace: bool = False
    ) -> pd.DataFrame:
        self._logger.debug("Interpolating is requested")
        if not inplace:
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
