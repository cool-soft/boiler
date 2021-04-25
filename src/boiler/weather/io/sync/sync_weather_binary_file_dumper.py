import logging
import os
from typing import Optional

import pandas as pd

from .sync_weather_binary_writer import SyncWeatherBinaryWriter
from .sync_weather_dumper import SyncWeatherDumper


class SyncWeatherBinaryFileDumper(SyncWeatherDumper):

    def __init__(self,
                 filepath: Optional[str] = None,
                 writer: Optional[SyncWeatherBinaryWriter] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._filepath = filepath
        self._writer = writer

        self._logger.debug(f"Filepath is {filepath}")
        self._logger.debug(f"Writer is {writer}")

    def set_filepath(self, filepath: str) -> None:
        self._logger.debug(f"Filepath is set to {filepath}")
        self._filepath = filepath

    def set_writer(self, writer: SyncWeatherBinaryWriter) -> None:
        self._logger.debug(f"Writer is set to {writer}")
        self._writer = writer

    def dump_weather(self, weather_df: pd.DataFrame) -> None:
        filepath = os.path.abspath(self._filepath)
        self._logger.debug(f"Storing weather to {filepath}")
        with open(filepath, "wb") as binary_file:
            self._writer.write_weather_to_binary_io(binary_file, weather_df)
        self._logger.debug("Weather is stored")
