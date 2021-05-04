import logging
from typing import BinaryIO

import pandas as pd

from boiler.temp_graph.io.abstract_sync_temp_graph_writer import AbstractSyncTempGraphWriter


class SyncTempGraphCSVWriter(AbstractSyncTempGraphWriter):

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

    def write_temp_graph_to_binary_stream(self,
                                          binary_stream: BinaryIO,
                                          temp_graph_df: pd.DataFrame) -> None:
        self._logger.debug("Storing temp graph")
        temp_graph_df.to_csv(binary_stream, encoding=self._encoding, sep=self._separator, index=False)
        self._logger.debug("Temp graph is stored")
