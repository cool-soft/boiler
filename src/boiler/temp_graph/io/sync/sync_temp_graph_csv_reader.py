import logging
from typing import TextIO

import pandas as pd

from .sync_temp_graph_text_reader import SyncTempGraphTextReader


class SyncTempGraphCSVReader(SyncTempGraphTextReader):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

    def read_temp_graph_from_text_io(self, text_io: TextIO) -> pd.DataFrame:
        self._logger.debug("Loading temp graph")
        temp_graph_df = pd.read_csv(text_io)
        self._logger.debug("Temp graph is loaded")
        return temp_graph_df
