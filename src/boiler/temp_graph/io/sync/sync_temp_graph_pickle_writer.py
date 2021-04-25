import logging
from typing import BinaryIO
import pickle

import pandas as pd

from .sync_temp_graph_binary_writer import SyncTempGraphBinaryWriter


class SyncTempGraphPickleWriter(SyncTempGraphBinaryWriter):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

    def write_temp_graph_to_binary_io(self, binary_io: BinaryIO, temp_graph_df: pd.DataFrame) -> None:
        self._logger.debug("Storing temp graph")
        pickle.dump(temp_graph_df, binary_io)
        self._logger.debug("Temp graph is stored")
