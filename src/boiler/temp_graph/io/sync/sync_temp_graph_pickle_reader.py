import logging
from typing import BinaryIO
import pickle

import pandas as pd

from .sync_temp_graph_binary_reader import SyncTempGraphBinaryReader


class SyncTempGraphPickleReader(SyncTempGraphBinaryReader):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

    def read_temp_graph_from_binary_io(self, binary_io: BinaryIO) -> pd.DataFrame:
        self._logger.debug("Loading temp graph")
        temp_graph_df = pickle.load(binary_io)
        self._logger.debug("Temp graph is loaded")
        return temp_graph_df
