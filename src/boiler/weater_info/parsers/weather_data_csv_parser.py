import logging
from typing import Union, IO

import pandas as pd

from .weather_data_parser import WeatherDataParser
from ...constants import column_names


class WeatherDataCSVParser(WeatherDataParser):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

    def parse_weather_data(self, weather_data: Union[str, IO]) -> pd.DataFrame:
        self._logger.debug("Parsing weather data")
        weather_df = pd.read_csv(weather_data, parse_dates=[column_names.TIMESTAMP])
        return weather_df
