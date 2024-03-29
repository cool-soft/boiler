import pickle
from typing import BinaryIO

import pandas as pd
from boiler.logging import logger

from boiler.weather.io.abstract_sync_weather_writer import AbstractSyncWeatherWriter


class SyncWeatherPickleWriter(AbstractSyncWeatherWriter):

    def __init__(self) -> None:
        logger.debug("Creating instance")

    def write_weather_to_binary_stream(self,
                                       binary_stream: BinaryIO,
                                       weather_df: pd.DataFrame
                                       ) -> None:
        logger.debug("Storing weather")
        pickle.dump(weather_df, binary_stream)
