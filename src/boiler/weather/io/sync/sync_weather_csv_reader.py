import logging
from typing import TextIO

import pandas as pd

from .sync_weather_text_reader import SyncWeatherTextReader
from boiler.constants import column_names


class SyncWeatherCSVReader(SyncWeatherTextReader):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

    def read_weather_from_text_io(self, text_io: TextIO) -> pd.DataFrame:
        self._logger.debug("Loading weather")
        weather_df = pd.read_csv(text_io, parse_dates=[column_names.TIMESTAMP])
        self._logger.debug("Weather is loaded")
        return weather_df
