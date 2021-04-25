import io
import logging
from typing import BinaryIO

import pandas as pd

from .sync_weather_writer import SyncWeatherWriter


class SyncWeatherCSVWriter(SyncWeatherWriter):

    def __init__(self, encoding="utf-8") -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._encoding = encoding

        self._logger.debug(f"Encoding is {encoding}")

    def set_encoding(self, encoding: str) -> None:
        self._logger.debug(f"Encoding is set to {encoding}")
        self._encoding = encoding

    def write_weather_to_binary_stream(self, binary_stream: BinaryIO, weather_df: pd.DataFrame) -> None:
        self._logger.debug("Storing weather")
        with io.TextIOWrapper(binary_stream, encoding=self._encoding) as text_stream:
            weather_df.to_csv(text_stream, index=False)
        self._logger.debug("Weather is stored")
