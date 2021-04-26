import logging
from typing import List, Optional

import pandas as pd

from boiler.constants import column_names
from .weather_data_processor import WeatherDataProcessor


class WeatherDataLinearInterpolator(WeatherDataProcessor):

    def __init__(self,
                 interpolation_step: pd.Timedelta = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._columns_to_interpolate = [column_names.WEATHER_TEMP]
        self._interpolation_step = interpolation_step

        self._logger.debug(f"Interpolation step is {interpolation_step}")

    def set_columns_to_interpolate(self, columns: List[str]) -> None:
        self._logger.debug(f"Columns to interpolate is set to {columns}")
        self._columns_to_interpolate = columns

    def set_interpolation_step(self, interpolation_step: pd.Timedelta) -> None:
        self._logger.debug(f"Interpolation step is set to {interpolation_step}")
        self._interpolation_step = interpolation_step

    def process_weather_df(self,
                           weather_df: pd.DataFrame,
                           start_datetime: Optional[pd.Timestamp] = None,
                           end_datetime: Optional[pd.Timestamp] = None,
                           inplace: bool = False) -> pd.DataFrame:
        self._logger.debug("Interpolating is requested")
        if not inplace:
            weather_df = weather_df.copy()
        weather_df.sort_values(by=column_names.TIMESTAMP, ignore_index=True, inplace=True)
        weather_df = self._interpolate_passes_of_datetime(weather_df)
        self._interpolate_passes_of_data(weather_df)
        self._logger.debug("Interpolated")
        return weather_df

    # TODO: вынести в отдельный алгоритм для weather и heating_obj
    def _interpolate_passes_of_datetime(self, weather_df: pd.DataFrame):
        self._logger.debug("Interpolating passes of datetime")

        datetime_to_insert = []
        previous_datetime = None
        for timestamp in weather_df[column_names.TIMESTAMP].to_list():
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

        weather_df = weather_df.append(datetime_to_insert, ignore_index=True)
        weather_df.sort_values(by=column_names.TIMESTAMP, ignore_index=True, inplace=True)

        return weather_df

    # TODO: вынести в отдельный алгоритм для weather и heating_obj
    def _interpolate_passes_of_data(self, weather_df: pd.DataFrame) -> None:
        self._logger.debug("Interpolating passes of data")
        for column_to_interpolate in self._columns_to_interpolate:
            weather_df[column_to_interpolate] = pd.to_numeric(weather_df[column_to_interpolate], downcast="float")
            weather_df[column_to_interpolate].interpolate(inplace=True)
