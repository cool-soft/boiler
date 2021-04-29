import logging
from typing import Optional

import pandas as pd

from boiler.constants import dataset_prototypes
from boiler.data_processing.processing_algo.processing import filter_by_timestamp_closed
from boiler.weather.io.sync.sync_weather_dumper import SyncWeatherDumper
from boiler.weather.io.sync.sync_weather_loader import SyncWeatherLoader


class SyncWeatherInMemoryDumperLoader(SyncWeatherLoader, SyncWeatherDumper):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._storage = dataset_prototypes.WEATHER.copy()

    def load_weather(self,
                     start_datetime: Optional[pd.Timestamp] = None,
                     end_datetime: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        self._logger.debug("Requested weather")
        weather_df = self._storage.copy()
        weather_df = filter_by_timestamp_closed(weather_df, start_datetime, end_datetime)
        return weather_df

    def dump_weather(self, weather_df: pd.DataFrame) -> None:
        self._logger.debug("Storing weather")
        self._storage = weather_df.copy()
