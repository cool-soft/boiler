import logging
from typing import TextIO

import pandas as pd

from .sync_weather_text_writer import SyncWeatherTextWriter


class SyncWeatherCSVWriter(SyncWeatherTextWriter):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

    def write_weather_to_text_io(self, text_io: TextIO, weather_df: pd.DataFrame) -> None:
        self._logger.debug("Storing weather")
        weather_df.to_csv(text_io, index=False)
        self._logger.debug("Weather is stored")
