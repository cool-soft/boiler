from typing import Optional

import pandas as pd
from boiler.logging import logger

from boiler.constants import dataset_prototypes
from boiler.data_processing.beetween_filter_algorithm import AbstractTimestampFilterAlgorithm, \
    LeftClosedTimestampFilterAlgorithm
from boiler.weather.io.abstract_sync_weather_dumper import AbstractSyncWeatherDumper
from boiler.weather.io.abstract_sync_weather_loader import AbstractSyncWeatherLoader


class SyncWeatherInMemoryDumperLoader(AbstractSyncWeatherDumper, AbstractSyncWeatherLoader):

    def __init__(self,
                 timestamp_filter_algorithm: AbstractTimestampFilterAlgorithm =
                 LeftClosedTimestampFilterAlgorithm()
                 ) -> None:
        self._storage = dataset_prototypes.WEATHER.copy()
        self._filter_algorithm = timestamp_filter_algorithm

        logger.debug(
            f"Creating instance:"
            f"Filter algorithm: {self._filter_algorithm}"
        )

    def load_weather(self,
                     start_datetime: Optional[pd.Timestamp] = None,
                     end_datetime: Optional[pd.Timestamp] = None
                     ) -> pd.DataFrame:
        logger.debug("Loading weather")
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
        logger.debug("Storing weather")
        self._storage = weather_df.copy()
