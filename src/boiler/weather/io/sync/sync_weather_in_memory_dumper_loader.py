import logging
from typing import Optional

import pandas as pd

from boiler.constants import dataset_prototypes
from boiler.data_processing.beetween_filter_algorithm import AbstractTimestampFilterAlgorithm, \
    LeftClosedTimestampFilterAlgorithm
from boiler.weather.io.sync.sync_weather_dumper import SyncWeatherDumper
from boiler.weather.io.sync.sync_weather_loader import SyncWeatherLoader


class SyncWeatherInMemoryDumperLoader(SyncWeatherLoader, SyncWeatherDumper):

    def __init__(self,
                 timestamp_filter_algorithm: AbstractTimestampFilterAlgorithm =
                 LeftClosedTimestampFilterAlgorithm()
                 ) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)

        self._storage = dataset_prototypes.WEATHER.copy()
        self._filter_algorithm = timestamp_filter_algorithm

    def load_weather(self,
                     start_datetime: Optional[pd.Timestamp] = None,
                     end_datetime: Optional[pd.Timestamp] = None
                     ) -> pd.DataFrame:
        weather_df = self._load_from_storage()
        weather_df = self._filter_by_timestamp(end_datetime, start_datetime, weather_df)
        return weather_df

    def _filter_by_timestamp(self, end_datetime, start_datetime, weather_df):
        weather_df = self._filter_algorithm.filter_df_by_min_max_timestamp(
            weather_df,
            start_datetime,
            end_datetime
        )
        return weather_df

    def _load_from_storage(self):
        weather_df = self._storage.copy()
        return weather_df

    def dump_weather(self, weather_df: pd.DataFrame) -> None:
        self._logger.debug("Storing weather")
        self._storage = weather_df.copy()
