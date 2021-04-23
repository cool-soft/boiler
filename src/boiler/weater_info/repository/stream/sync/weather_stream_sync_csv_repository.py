import logging
import os
from typing import Optional

import pandas as pd

from boiler.parsing_utils.utils import filter_by_timestamp_closed
from boiler.weater_info.parsers.weather_data_parser import WeatherDataParser
from .weather_stream_sync_repository import WeatherStreamSyncRepository


class WeatherStreamSyncCSVRepository(WeatherStreamSyncRepository):

    def __init__(self,
                 filepath: str = "./storage/weather.csv",
                 parser: Optional[WeatherDataParser] = None,
                 encoding="utf-8") -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._filepath = filepath
        self._parser = parser
        self._encoding = encoding

        self._logger.debug(f"Filepath is {filepath}")
        self._logger.debug(f"Parse is {parser}")
        self._logger.debug(f"Encoding is {encoding}")

    def set_filepath(self, filepath: str) -> None:
        self._logger.debug(f"Filepath is set to {filepath}")
        self._filepath = filepath

    def set_parser(self, parser: WeatherDataParser) -> None:
        self._logger.debug(f"Parser is set to {parser}")
        self._parser = parser

    def set_encoding(self, encoding: str) -> None:
        self._logger.debug(f"Encoding is set to {encoding}")
        self._encoding = encoding

    def get_weather_info(self,
                         start_datetime: Optional[pd.Timestamp] = None,
                         end_datetime: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        filepath = os.path.abspath(self._filepath)
        self._logger.debug(f"Loading weather info from {filepath}")
        with open(filepath, mode="r", encoding=self._encoding) as input_file:
            weather_df = self._parser.parse_weather_data(input_file)
        weather_df = filter_by_timestamp_closed(weather_df, start_datetime, end_datetime)
        self._logger.debug("Weather info is loaded")
        return weather_df

    def set_weather_info(self, weather_df: pd.DataFrame) -> None:
        filepath = os.path.abspath(self._filepath)
        self._logger.debug(f"Storing weather info to {filepath}")
        with open(filepath, mode="w", encoding=self._encoding) as output_file:
            weather_df.to_csv(output_file, index=False)
        self._logger.debug("Weather info is stored")
