import logging
import os
from typing import Optional

import pandas as pd

from boiler.data_processing.beetween_filter_algorithm import \
    AbstractTimestampFilterAlgorithm, LeftClosedTimestampFilterAlgorithm
from boiler.weather.io.abstract_sync_weather_loader import AbstractSyncWeatherLoader
from boiler.weather.io.abstract_sync_weather_reader import AbstractSyncWeatherReader


class SyncWeatherFileLoader(AbstractSyncWeatherLoader):

    def __init__(self,
                 filepath: str,
                 reader: AbstractSyncWeatherReader,
                 timestamp_filter_algorithm: AbstractTimestampFilterAlgorithm =
                 LeftClosedTimestampFilterAlgorithm()) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)

        self._filepath = filepath
        self._reader = reader
        self._filter_algorithm = timestamp_filter_algorithm

    def load_weather(self,
                     start_datetime: Optional[pd.Timestamp] = None,
                     end_datetime: Optional[pd.Timestamp] = None
                     ) -> pd.DataFrame:
        weather_df = self._load_from_file()
        weather_df = self._filter_by_timestamp(end_datetime, start_datetime, weather_df)
        self._logger.debug("Weather is loaded")
        return weather_df

    def _load_from_file(self):
        filepath = os.path.abspath(self._filepath)
        self._logger.debug(f"Loading weather from {filepath}")
        with open(filepath, mode="rb") as input_file:
            weather_df = self._reader.read_weather_from_binary_stream(input_file)
        return weather_df

    def _filter_by_timestamp(self, end_datetime, start_datetime, weather_df):
        weather_df = self._filter_algorithm.filter_df_by_min_max_timestamp(
            weather_df,
            start_datetime,
            end_datetime
        )
        return weather_df
