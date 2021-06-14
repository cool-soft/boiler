from typing import BinaryIO

import pandas as pd
from boiler.logger import logger

from boiler.temp_graph.io.abstract_sync_temp_graph_writer import AbstractSyncTempGraphWriter


class SyncTempGraphCSVWriter(AbstractSyncTempGraphWriter):

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

    def write_temp_graph_to_binary_stream(self,
                                          binary_stream: BinaryIO,
                                          temp_graph_df: pd.DataFrame) -> None:
        logger.debug("Storing temp graph")
        temp_graph_df.to_csv(binary_stream, encoding=self._encoding, sep=self._separator, index=False)
