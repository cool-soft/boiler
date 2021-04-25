import logging
import os
from typing import Optional

import pandas as pd

from .sync_temp_graph_binary_reader import SyncTempGraphBinaryReader
from .sync_temp_graph_loader import SyncTempGraphLoader


class SyncTempGraphBinaryFileLoader(SyncTempGraphLoader):

    def __init__(self,
                 filepath: Optional[str] = None,
                 reader: Optional[SyncTempGraphBinaryReader] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._filepath = filepath
        self._reader = reader

        self._logger.debug(f"Filepath is {filepath}")
        self._logger.debug(f"Reader is {reader}")

    def set_filepath(self, filepath: str) -> None:
        self._logger.debug(f"Filepath is set to {filepath}")
        self._filepath = filepath

    def set_reader(self, reader: SyncTempGraphBinaryReader) -> None:
        self._logger.debug(f"Reader is set to {reader}")
        self._reader = reader

    def load_temp_graph(self) -> pd.DataFrame:
        filepath = os.path.abspath(self._filepath)
        self._logger.debug(f"Loading temp graph from {filepath}")
        with open(filepath, "rb") as binary_file:
            weather_df = self._reader.read_temp_graph_from_binary_io(binary_file)
        self._logger.debug("Temp graph is loaded")
        return weather_df
