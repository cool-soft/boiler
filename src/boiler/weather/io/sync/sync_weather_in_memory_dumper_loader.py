import logging
from typing import Optional

import pandas as pd

from boiler.constants import column_names
from boiler.data_processing.processing_algo.processing import filter_by_timestamp_closed
from boiler.weather.io.sync.sync_weather_dumper import SyncWeatherDumper
from boiler.weather.io.sync.sync_weather_loader import SyncWeatherLoader


class SyncWeatherInMemoryDumperLoader(SyncWeatherLoader, SyncWeatherDumper):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        # TODO: вынести создание пустого DataFrame с заданными колонками куда-нибудь
        self._storage = pd.DataFrame(
            columns=(column_names.TIMESTAMP,
                     column_names.WEATHER_TEMP)
        )

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
