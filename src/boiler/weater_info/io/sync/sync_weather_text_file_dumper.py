import logging
import os
from typing import Optional

import pandas as pd

from .sync_weather_dumper import SyncWeatherDumper
from .sync_weather_text_writer import SyncWeatherTextWriter


class SyncWeatherTextFileDumper(SyncWeatherDumper):

    def __init__(self,
                 filepath: Optional[str] = None,
                 writer: Optional[SyncWeatherTextWriter] = None,
                 encoding="utf-8") -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._filepath = filepath
        self._writer = writer
        self._encoding = encoding

        self._logger.debug(f"Filepath is {filepath}")
        self._logger.debug(f"Writer is {writer}")
        self._logger.debug(f"Encoding is {encoding}")

    def set_filepath(self, filepath: str) -> None:
        self._logger.debug(f"Filepath is set to {filepath}")
        self._filepath = filepath

    def set_writer(self, writer: SyncWeatherTextWriter) -> None:
        self._logger.debug(f"Writer is set to {writer}")
        self._writer = writer

    def set_encoding(self, encoding: str) -> None:
        self._logger.debug(f"Encoding is set to {encoding}")
        self._encoding = encoding

    def dump_weather(self, weather_df: pd.DataFrame) -> None:
        filepath = os.path.abspath(self._filepath)
        self._logger.debug(f"Storing weather to {filepath}")
        with open(filepath, mode="w", encoding=self._encoding) as output_file:
            self._writer.write_weather_to_text_io(output_file, weather_df)
        self._logger.debug("Weather is stored")
