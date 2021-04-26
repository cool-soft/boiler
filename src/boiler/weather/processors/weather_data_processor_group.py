import logging
from typing import List, Optional

import pandas as pd

from .weather_data_processor import WeatherDataProcessor


class WeatherDataProcessorsGroup(WeatherDataProcessor):

    def __init__(self,
                 processors: Optional[List[WeatherDataProcessor]] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        if processors is None:
            processors = []
        self._processors = processors.copy()

        self._logger.debug(f"Found {len(processors)} processors")

    def set_processors(self, processors: List[WeatherDataProcessor]) -> None:
        self._logger.debug("Processors is set")
        self._processors = processors.copy()
        self._logger.debug(f"Found {len(processors)} processors")

    def process_weather_df(self,
                           weather_df: pd.DataFrame,
                           start_datetime: Optional[pd.Timestamp] = None,
                           end_datetime: Optional[pd.Timestamp] = None,
                           inplace: bool = False) -> pd.DataFrame:
        self._logger.debug("Requested processing")

        for processor in self._processors:
            weather_df = processor.process_weather_df(
                weather_df,
                start_datetime,
                end_datetime,
                inplace
            )

        self._logger.debug("Processed")
        return weather_df
