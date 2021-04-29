import logging
from typing import Optional

import pandas as pd

from boiler.constants import dataset_prototypes
from boiler.data_processing.processing_algo.beetween_filter_algorithm import AbstractBetweenFilterAlgorithm
from boiler.weather.io.sync.sync_weather_dumper import SyncWeatherDumper
from boiler.weather.io.sync.sync_weather_loader import SyncWeatherLoader


class SyncWeatherInMemoryDumperLoader(SyncWeatherLoader, SyncWeatherDumper):

    def __init__(self,
                 filter_algorithm: Optional[AbstractBetweenFilterAlgorithm] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._storage = dataset_prototypes.WEATHER.copy()
        self._filter_algorithm = filter_algorithm

    def set_filter_algorithm(self, algorithm: AbstractBetweenFilterAlgorithm):
        self._logger.debug(f"Filter algorithm is set to {algorithm}")
        self._filter_algorithm = algorithm

    def load_weather(self,
                     start_datetime: Optional[pd.Timestamp] = None,
                     end_datetime: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        self._logger.debug("Requested weather")
        weather_df = self._storage.copy()
        weather_df = self._filter_algorithm.filter_df_by_min_max_values(
            weather_df,
            start_datetime,
            end_datetime
        )
        return weather_df

    def dump_weather(self, weather_df: pd.DataFrame) -> None:
        self._logger.debug("Storing weather")
        self._storage = weather_df.copy()
