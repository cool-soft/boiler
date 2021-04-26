import logging
import pickle
from typing import BinaryIO

import pandas as pd

from boiler.weather.io.sync.sync_weather_reader import SyncWeatherReader


class SyncWeatherPickleReader(SyncWeatherReader):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

    def read_weather_from_binary_stream(self, binary_stream: BinaryIO) -> pd.DataFrame:
        self._logger.debug("Loading weather")
        weather_df = pickle.load(binary_stream)
        self._logger.debug("Weather is loaded")
        return weather_df
