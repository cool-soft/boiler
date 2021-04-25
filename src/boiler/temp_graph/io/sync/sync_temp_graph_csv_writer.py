import logging
from typing import TextIO

import pandas as pd

from .sync_temp_graph_text_writer import SyncTempGraphTextWriter


class SyncTempGraphCSVWriter(SyncTempGraphTextWriter):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

    def write_temp_graph_to_text_io(self,
                                    text_io: TextIO,
                                    temp_graph_df: pd.DataFrame) -> None:
        self._logger.debug("Storing temp graph")
        temp_graph_df.to_csv(text_io, index=False)
        self._logger.debug("Temp graph is stored")
