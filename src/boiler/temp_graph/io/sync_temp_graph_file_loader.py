import logging
import os

import pandas as pd

from boiler.temp_graph.io.abstract_sync_temp_graph_loader import AbstractSyncTempGraphLoader
from boiler.temp_graph.io.abstract_sync_temp_graph_reader import AbstractSyncTempGraphReader


class SyncTempGraphFileLoader(AbstractSyncTempGraphLoader):

    def __init__(self,
                 filepath: str,
                 reader: AbstractSyncTempGraphReader
                 ) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._filepath = filepath
        self._reader = reader

        self._logger.debug(f"Filepath is {filepath}")
        self._logger.debug(f"Reader is {reader}")

    def load_temp_graph(self) -> pd.DataFrame:
        filepath = os.path.abspath(self._filepath)
        self._logger.debug(f"Loading temp graph from {filepath}")
        with open(filepath, mode="rb") as input_file:
            temp_graph_df = self._reader.read_temp_graph_from_binary_stream(input_file)
        self._logger.debug("Temp graph is loaded")
        return temp_graph_df