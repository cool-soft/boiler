import logging
import os

import pandas as pd

from boiler.weather.io.abstract_sync_weather_dumper import AbstractSyncWeatherDumper
from boiler.weather.io.abstract_sync_weather_writer import AbstractSyncWeatherWriter


class SyncWeatherFileDumper(AbstractSyncWeatherDumper):

    def __init__(self,
                 filepath: str,
                 writer: AbstractSyncWeatherWriter,
                 ) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._filepath = filepath
        self._writer = writer

        self._logger.debug(f"Filepath is {filepath}")
        self._logger.debug(f"Writer is {writer}")

    def dump_weather(self, weather_df: pd.DataFrame) -> None:
        filepath = os.path.abspath(self._filepath)
        self._logger.debug(f"Storing weather to {filepath}")
        with open(filepath, mode="wb") as output_file:
            self._writer.write_weather_to_binary_stream(output_file, weather_df)
        self._logger.debug("Weather is stored")
