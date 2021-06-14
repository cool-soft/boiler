from typing import BinaryIO

import pandas as pd
from boiler.logger import logger

from boiler.temp_graph.io.abstract_sync_temp_graph_reader import AbstractSyncTempGraphReader


class SyncTempGraphCSVReader(AbstractSyncTempGraphReader):

    def __init__(self,
                 encoding: str = "utf-8",
                 separator: str = ";"
                 ) -> None:

        self._encoding = encoding
        self._separator = separator

        logger.debug(
            f"Creating instance:"
            f"encoding: {encoding}"
            f"separator: {separator}"
        )

    def read_temp_graph_from_binary_stream(self,
                                           binary_stream: BinaryIO
                                           ) -> pd.DataFrame:
        logger.debug("Loading temp graph")
        temp_graph_df = pd.read_csv(
            binary_stream,
            encoding=self._encoding,
            sep=self._separator
        )
        return temp_graph_df
