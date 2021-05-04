import logging
from typing import BinaryIO

import pandas as pd

from boiler.weather.io.abstract_sync_weather_writer import AbstractSyncWeatherWriter


class SyncWeatherCSVWriter(AbstractSyncWeatherWriter):

    def __init__(self,
                 encoding: str = "utf-8",
                 separator: str = ";"
                 ) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._encoding = encoding
        self._separator = separator

        self._logger.debug(f"Encoding is {encoding}")
        self._logger.debug(f"Separator is {separator}")

    def write_weather_to_binary_stream(self, binary_stream: BinaryIO, weather_df: pd.DataFrame) -> None:
        self._logger.debug("Storing weather")
        weather_df.to_csv(
            binary_stream,
            encoding=self._encoding,
            sep=self._separator,
            index=False
        )
        self._logger.debug("Weather is stored")
