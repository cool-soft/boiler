import os

import pandas as pd
from boiler.logger import boiler_logger

from boiler.temp_graph.io.abstract_sync_temp_graph_loader import AbstractSyncTempGraphLoader
from boiler.temp_graph.io.abstract_sync_temp_graph_reader import AbstractSyncTempGraphReader


class SyncTempGraphFileLoader(AbstractSyncTempGraphLoader):

    def __init__(self,
                 filepath: str,
                 reader: AbstractSyncTempGraphReader
                 ) -> None:
        self._filepath = filepath
        self._reader = reader

        boiler_logger.debug(
            f"Creating instance:"
            f"filepath: {filepath}"
            f"reader: {reader}"
        )

    def load_temp_graph(self) -> pd.DataFrame:
        filepath = os.path.abspath(self._filepath)
        boiler_logger.debug(f"Loading temp graph from {filepath}")
        with open(filepath, mode="rb") as input_file:
            temp_graph_df = self._reader.read_temp_graph_from_binary_stream(input_file)
        return temp_graph_df
