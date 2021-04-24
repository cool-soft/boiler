import logging
from typing import BinaryIO
import pickle

import pandas as pd

from .sync_weather_binary_writer import SyncWeatherBinaryWriter


class SyncWeatherPickleWriter(SyncWeatherBinaryWriter):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

    def write_weather_to_binary_io(self, binary_io: BinaryIO, weather_df: pd.DataFrame) -> None:
        self._logger.debug("Storing weather")
        pickle.dump(weather_df, binary_io)
        self._logger.debug("Weather is stored")
