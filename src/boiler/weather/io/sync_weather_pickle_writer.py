import logging
import pickle
from typing import BinaryIO

import pandas as pd

from boiler.weather.io.abstract_sync_weather_writer import AbstractSyncWeatherWriter


class SyncWeatherPickleWriter(AbstractSyncWeatherWriter):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

    def write_weather_to_binary_stream(self,
                                       binary_stream: BinaryIO,
                                       weather_df: pd.DataFrame
                                       ) -> None:
        self._logger.debug("Storing weather")
        pickle.dump(weather_df, binary_stream)
        self._logger.debug("Weather is stored")
