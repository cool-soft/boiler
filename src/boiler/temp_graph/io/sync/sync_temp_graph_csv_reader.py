import io
import logging
from typing import BinaryIO

import pandas as pd

from boiler.temp_graph.io.sync.sync_temp_graph_reader import SyncTempGraphReader


class SyncTempGraphCSVReader(SyncTempGraphReader):

    def __init__(self, encoding="utf-8") -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._encoding = encoding

        self._logger.debug(f"Encoding is {encoding}")

    def set_encoding(self, encoding: str) -> None:
        self._logger.debug(f"Encoding is set to {encoding}")
        self._encoding = encoding

    def read_temp_graph_from_binary_stream(self, binary_stream: BinaryIO) -> pd.DataFrame:
        self._logger.debug("Loading temp graph")
        with io.TextIOWrapper(binary_stream, encoding=self._encoding) as text_stream:
            temp_graph_df = pd.read_csv(text_stream)
        self._logger.debug("Temp graph is loaded")
        return temp_graph_df
