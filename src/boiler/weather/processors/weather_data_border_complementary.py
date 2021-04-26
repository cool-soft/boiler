import logging
from typing import List, Optional

import pandas as pd

from boiler.constants import column_names
from boiler.utils.processing_utils import TimestampRoundAlgo
from .weather_data_processor import WeatherDataProcessor


class WeatherDataBorderComplementary(WeatherDataProcessor):

    def __init__(self,
                 timestamp_round_algo: Optional[TimestampRoundAlgo] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._columns_to_complementary = [column_names.WEATHER_TEMP]
        self._timestamp_round_algo = timestamp_round_algo

        self._logger.debug(f"Timestamp round algo {timestamp_round_algo}")

    def set_columns_to_complementary(self, columns: List[str]) -> None:
        self._logger.debug(f"Columns to complementary is set to {columns}")
        self._columns_to_complementary = columns

    def set_timestamp_round_algo(self, round_algo: TimestampRoundAlgo) -> None:
        self._logger.debug(f"Round algo is set to {round_algo}")
        self._timestamp_round_algo = round_algo

    def process_weather_df(self,
                           weather_df: pd.DataFrame,
                           start_datetime: Optional[pd.Timestamp] = None,
                           end_datetime: Optional[pd.Timestamp] = None,
                           inplace: bool = False) -> pd.DataFrame:
        self._logger.debug("Interpolating is requested")
        if not inplace:
            weather_df = weather_df.copy()
        weather_df = self._interpolate_border_datetime(weather_df, start_datetime, end_datetime)
        self._interpolate_border_data(weather_df)
        self._logger.debug("Interpolated")
        return weather_df

    def _interpolate_border_datetime(self,
                                     weather_df: pd.DataFrame,
                                     start_datetime: pd.Timestamp,
                                     end_datetime: pd.Timestamp) -> pd.DataFrame:
        self._logger.debug("Interpolating border datetime values")

        if start_datetime is not None:
            weather_df = self._interpolate_border_start_timestamp(start_datetime, weather_df)

        if end_datetime is not None:
            weather_df = self._interpolate_border_end_timestamp(end_datetime, weather_df)

        return weather_df

    def _interpolate_border_start_timestamp(self, start_datetime: pd.Timestamp, weather_df: pd.DataFrame):
        start_datetime = self._timestamp_round_algo.round_value(start_datetime)
        first_datetime_idx = weather_df[column_names.TIMESTAMP].idxmin()
        first_row = weather_df.loc[first_datetime_idx]
        first_datetime = first_row[column_names.TIMESTAMP]
        if first_datetime > start_datetime:
            weather_df = weather_df.append({column_names.TIMESTAMP: start_datetime}, ignore_index=True)
        return weather_df

    def _interpolate_border_end_timestamp(self, end_datetime: pd.Timestamp, weather_df: pd.DataFrame):
        end_datetime = self._timestamp_round_algo.round_value(end_datetime)
        last_datetime_idx = weather_df[column_names.TIMESTAMP].idxmax()
        last_row = weather_df.loc[last_datetime_idx]
        last_datetime = last_row[column_names.TIMESTAMP]
        if last_datetime < end_datetime:
            weather_df = weather_df.append({column_names.TIMESTAMP: end_datetime}, ignore_index=True)
        return weather_df

    def _interpolate_border_data(self, weather_df: pd.DataFrame) -> None:
        self._logger.debug("Interpolating border data values")

        first_datetime_index = weather_df[column_names.TIMESTAMP].idxmin()
        last_datetime_index = weather_df[column_names.TIMESTAMP].idxmax()

        for column_name in self._columns_to_complementary:
            self._interpolate_column_border_start_value(column_name, first_datetime_index, weather_df)
            self._interpolate_column_border_end_value(column_name, last_datetime_index, weather_df)

    # TODO: передавать колонку, вместо имени
    # noinspection PyMethodMayBeStatic
    def _interpolate_column_border_end_value(self, column_name, last_datetime_index, weather_df):
        last_valid_index = weather_df[column_name].last_valid_index()
        if last_valid_index != last_datetime_index:
            last_valid_value = weather_df.loc[last_valid_index, column_name]
            weather_df.loc[last_datetime_index, column_name] = last_valid_value

    # TODO: передавать колонку, вместо имени
    # noinspection PyMethodMayBeStatic
    def _interpolate_column_border_start_value(self, column_name, first_datetime_index, weather_df):
        first_valid_index = weather_df[column_name].first_valid_index()
        if first_valid_index != first_datetime_index:
            first_valid_value = weather_df.loc[first_valid_index, column_name]
            weather_df.loc[first_datetime_index, column_name] = first_valid_value
