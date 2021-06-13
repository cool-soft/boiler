from typing import BinaryIO

import pandas as pd
from boiler.logger import boiler_logger

from boiler.weather.io.abstract_sync_weather_writer import AbstractSyncWeatherWriter


class SyncWeatherCSVWriter(AbstractSyncWeatherWriter):

    def __init__(self,
                 encoding: str = "utf-8",
                 separator: str = ";"
                 ) -> None:
        self._encoding = encoding
        self._separator = separator

        boiler_logger.debug(
            f"Creating instance:"
            f"encoding: {encoding}"
            f"separator: {separator}"
        )

    def write_weather_to_binary_stream(self, binary_stream: BinaryIO, weather_df: pd.DataFrame) -> None:
        boiler_logger.debug("Storing weather")
        weather_df.to_csv(
            binary_stream,
            encoding=self._encoding,
            sep=self._separator,
            index=False
        )
