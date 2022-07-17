import os

import pandas as pd
from boiler.logging import logger

from boiler.weather.io.abstract_sync_weather_dumper import AbstractSyncWeatherDumper
from boiler.weather.io.abstract_sync_weather_writer import AbstractSyncWeatherWriter


class SyncWeatherFileDumper(AbstractSyncWeatherDumper):

    def __init__(self,
                 filepath: str,
                 writer: AbstractSyncWeatherWriter,
                 ) -> None:
        self._filepath = filepath
        self._writer = writer

        logger.debug(
            f"Creating instance:"
            f"filepath: {filepath}"
            f"writer: {writer}"
        )

    def dump_weather(self, weather_df: pd.DataFrame) -> None:
        filepath = os.path.abspath(self._filepath)
        logger.debug(f"Storing weather to {filepath}")
        with open(filepath, mode="wb") as output_file:
            self._writer.write_weather_to_binary_stream(output_file, weather_df)
