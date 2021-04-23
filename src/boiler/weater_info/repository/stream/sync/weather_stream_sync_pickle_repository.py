import logging
import os
from typing import Optional

import pandas as pd

from boiler.parsing_utils.utils import filter_by_timestamp_closed
from .weather_stream_sync_repository import WeatherStreamSyncRepository


class WeatherStreamSyncPickleRepository(WeatherStreamSyncRepository):

    def __init__(self,
                 filepath: str = "./storage/weather.pickle") -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._filepath = filepath

        self._logger.debug(f"Filepath is {filepath}")

    def set_filepath(self, filepath: str) -> None:
        self._logger.debug(f"Filepath is set to {filepath}")
        self._filepath = filepath

    def get_weather_info(self,
                         start_datetime: Optional[pd.Timestamp] = None,
                         end_datetime: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        filepath = os.path.abspath(self._filepath)
        self._logger.debug(f"Loading weather info from {filepath} from {start_datetime} to {end_datetime}")
        weather_df = pd.read_pickle(filepath)
        weather_df = filter_by_timestamp_closed(weather_df, start_datetime, end_datetime)
        self._logger.debug("Weather info is loaded")
        return weather_df

    def set_weather_info(self, weather_df: pd.DataFrame) -> None:
        filepath = os.path.abspath(self._filepath)
        self._logger.debug(f"Storing weather info to {filepath}")
        weather_df.to_pickle(filepath)
        self._logger.debug("Weather info is stored")
