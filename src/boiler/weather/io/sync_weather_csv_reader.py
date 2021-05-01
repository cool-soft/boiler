import io
import logging
from typing import BinaryIO

import pandas as pd

from boiler.weather.io.abstract_sync_weather_reader import AbstractSyncWeatherReader
from boiler.constants import column_names


class SyncWeatherCSVReader(AbstractSyncWeatherReader):

    def __init__(self, encoding: str = "utf-8") -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._encoding = encoding

        self._logger.debug(f"Encoding is {encoding}")

    def read_weather_from_binary_stream(self, binary_stream: BinaryIO) -> pd.DataFrame:
        self._logger.debug("Loading weather")
        with io.TextIOWrapper(binary_stream, encoding=self._encoding) as text_stream:
            weather_df = pd.read_csv(text_stream, parse_dates=[column_names.TIMESTAMP])
        self._logger.debug("Weather is loaded")
        return weather_df
