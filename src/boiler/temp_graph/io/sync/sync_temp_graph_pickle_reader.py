import logging
import pickle
from typing import BinaryIO

import pandas as pd

from boiler.temp_graph.io.sync.sync_temp_graph_reader import SyncTempGraphReader


class SyncTempGraphPickleReader(SyncTempGraphReader):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

    def read_temp_graph_from_binary_stream(self, binary_stream: BinaryIO) -> pd.DataFrame:
        self._logger.debug("Loading temp graph")
        temp_graph_df = pickle.load(binary_stream)
        self._logger.debug("Temp graph is loaded")
        return temp_graph_df
