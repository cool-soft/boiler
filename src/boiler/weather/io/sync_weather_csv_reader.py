from typing import BinaryIO

import pandas as pd
from boiler.logger import logger

from boiler.constants import column_names
from boiler.weather.io.abstract_sync_weather_reader import AbstractSyncWeatherReader


class SyncWeatherCSVReader(AbstractSyncWeatherReader):

    def __init__(self,
                 encoding: str = "utf-8",
                 separator: str = ";"
                 ) -> None:
        self._encoding = encoding
        self._separator = separator

        logger.debug(
            f"Creating instance:"
            f"encoding: {encoding}"
            f"separator: {separator}"
        )

    def read_weather_from_binary_stream(self, binary_stream: BinaryIO) -> pd.DataFrame:
        logger.debug("Loading weather")
        weather_df = pd.read_csv(
            binary_stream,
            encoding=self._encoding,
            sep=self._separator,
            parse_dates=[column_names.TIMESTAMP]
        )
        return weather_df
