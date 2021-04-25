import logging
import os
from typing import Optional

import pandas as pd

from boiler.parsing_utils.utils import filter_by_timestamp_closed
from .sync_weather_reader import SyncWeatherReader
from .sync_weather_loader import SyncWeatherLoader


class SyncWeatherFileLoader(SyncWeatherLoader):

    def __init__(self,
                 filepath: Optional[str] = None,
                 reader: Optional[SyncWeatherReader] = None
                 ) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._filepath = filepath
        self._reader = reader

        self._logger.debug(f"Filepath is {filepath}")
        self._logger.debug(f"Reader is {reader}")

    def set_reader(self, reader: SyncWeatherReader):
        self._logger.debug(f"Reader is set to {reader}")
        self._reader = reader

    def load_weather(self,
                     start_datetime: Optional[pd.Timestamp] = None,
                     end_datetime: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        filepath = os.path.abspath(self._filepath)
        self._logger.debug(f"Loading weather from {filepath}")
        with open(filepath, mode="rb") as input_file:
            weather_df = self._reader.read_weather_from_binary_stream(input_file)
        weather_df = filter_by_timestamp_closed(weather_df, start_datetime, end_datetime)
        self._logger.debug("Weather is loaded")
        return weather_df
