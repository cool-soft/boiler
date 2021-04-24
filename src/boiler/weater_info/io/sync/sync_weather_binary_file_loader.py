import logging
import os
from typing import Optional

import pandas as pd

from .sync_weather_binary_reader import SyncWeatherBinaryReader
from .sync_weather_loader import SyncWeatherLoader


class SyncWeatherBinaryFileLoader(SyncWeatherLoader):

    def __init__(self,
                 filepath: Optional[str] = None,
                 reader: Optional[SyncWeatherBinaryReader] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._filepath = filepath
        self._reader = reader

        self._logger.debug(f"Filepath is {filepath}")
        self._logger.debug(f"Reader is {reader}")

    def set_filepath(self, filepath: str) -> None:
        self._logger.debug(f"Filepath is set to {filepath}")
        self._filepath = filepath

    def set_reader(self, reader: SyncWeatherBinaryReader) -> None:
        self._logger.debug(f"Reader is set to {reader}")
        self._reader = reader

    def load_weather(self,
                     start_datetime: Optional[pd.Timestamp] = None,
                     end_datetime: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        filepath = os.path.abspath(self._filepath)
        self._logger.debug(f"Loading weather from {filepath}")
        with open(filepath, "rb") as binary_file:
            weather_df = self._reader.read_weather_from_binary_io(binary_file)
        self._logger.debug("Weather is loaded")
        return weather_df
