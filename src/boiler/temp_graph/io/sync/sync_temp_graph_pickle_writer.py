import logging
import pickle
from typing import BinaryIO

import pandas as pd

from .sync_temp_graph_writer import SyncTempGraphWriter


class SyncTempGraphPickleWriter(SyncTempGraphWriter):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

    def write_temp_graph_to_binary_stream(self, binary_stream: BinaryIO, temp_graph_df: pd.DataFrame) -> None:
        self._logger.debug("Storing temp graph")
        pickle.dump(temp_graph_df, binary_stream)
        self._logger.debug("Temp graph is stored")
