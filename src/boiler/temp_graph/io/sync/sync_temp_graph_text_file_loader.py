import logging
import os
from typing import Optional

import pandas as pd

from .sync_temp_graph_loader import SyncTempGraphLoader
from .sync_temp_graph_text_reader import SyncTempGraphTextReader


class SyncTempGraphTextFileLoader(SyncTempGraphLoader):

    def __init__(self,
                 filepath: Optional[str] = None,
                 reader: Optional[SyncTempGraphTextReader] = None,
                 encoding="utf-8") -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._filepath = filepath
        self._reader = reader
        self._encoding = encoding

        self._logger.debug(f"Filepath is {filepath}")
        self._logger.debug(f"Reader is {reader}")
        self._logger.debug(f"Encoding is {encoding}")

    def set_encoding(self, encoding: str) -> None:
        self._logger.debug(f"Encoding is set to {encoding}")
        self._encoding = encoding

    def set_reader(self, reader: SyncTempGraphTextReader):
        self._logger.debug(f"Reader is set to {reader}")
        self._reader = reader

    def load_temp_graph(self) -> pd.DataFrame:
        filepath = os.path.abspath(self._filepath)
        self._logger.debug(f"Loading temp graph from {filepath}")
        with open(filepath, mode="r", encoding=self._encoding) as text_io:
            temp_graph_df = self._reader.read_temp_graph_from_text_io(text_io)
        self._logger.debug("Temp graph is loaded")
        return temp_graph_df
