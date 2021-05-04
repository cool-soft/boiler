import logging
from typing import BinaryIO

import pandas as pd

from boiler.temp_graph.io.abstract_sync_temp_graph_reader import AbstractSyncTempGraphReader


class SyncTempGraphCSVReader(AbstractSyncTempGraphReader):

    def __init__(self,
                 encoding: str = "utf-8",
                 separator: str = ";"
                 ) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._encoding = encoding
        self._separator = separator

        self._logger.debug(f"Encoding is {encoding}")
        self._logger.debug(f"Separator is {separator}")

    def read_temp_graph_from_binary_stream(self,
                                           binary_stream: BinaryIO
                                           ) -> pd.DataFrame:
        self._logger.debug("Loading temp graph")
        temp_graph_df = pd.read_csv(
            binary_stream,
            encoding=self._encoding,
            sep=self._separator
        )
        self._logger.debug("Temp graph is loaded")
        return temp_graph_df
