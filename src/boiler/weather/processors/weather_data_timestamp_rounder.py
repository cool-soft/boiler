import logging
from typing import Optional

import pandas as pd

from boiler.constants import column_names
from boiler.utils.processing_utils import TimestampRoundAlgo
from boiler.weather.processors.weather_data_processor import WeatherDataProcessor


class HeatingObjDataTimestampRounder(WeatherDataProcessor):

    def __init__(self,
                 round_algo: Optional[TimestampRoundAlgo] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._round_algo = round_algo

        self._logger.debug(f"Round algo is {round_algo}")

    def set_round_algo(self, round_algo: TimestampRoundAlgo) -> None:
        self._logger.debug(f"Round algo is set to {round_algo}")
        self._round_algo = round_algo

    def process_weather_df(self,
                           weather_df: pd.DataFrame,
                           start_datetime: Optional[pd.Timestamp] = None,
                           end_datetime: Optional[pd.Timestamp] = None,
                           inplace: bool = False) -> pd.DataFrame:
        self._logger.debug("Processing is requested")

        if not inplace:
            weather_df = weather_df.copy()
        weather_df[column_names.TIMESTAMP] = self._round_algo.round_series(weather_df[column_names.TIMESTAMP])
        weather_df.drop_duplicates(column_names.TIMESTAMP, inplace=True, ignore_index=True)

        self._logger.debug("Processed")
        return weather_df
