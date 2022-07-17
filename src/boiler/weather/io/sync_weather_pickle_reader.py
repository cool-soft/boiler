import pickle
from typing import BinaryIO

import pandas as pd
from boiler.logging import logger

from boiler.weather.io.abstract_sync_weather_reader import AbstractSyncWeatherReader


class SyncWeatherPickleReader(AbstractSyncWeatherReader):

    def __init__(self) -> None:
        logger.debug("Creating instance")

    def read_weather_from_binary_stream(self,
                                        binary_stream: BinaryIO
                                        ) -> pd.DataFrame:
        logger.debug("Loading weather")
        weather_df = pickle.load(binary_stream)
        return weather_df
