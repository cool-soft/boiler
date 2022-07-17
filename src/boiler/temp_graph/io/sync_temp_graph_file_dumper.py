import os

import pandas as pd
from boiler.logging import logger

from boiler.temp_graph.io.abstract_sync_temp_graph_dumper import AbstractSyncTempGraphDumper
from boiler.temp_graph.io.abstract_sync_temp_graph_writer import AbstractSyncTempGraphWriter


class SyncTempGraphFileDumper(AbstractSyncTempGraphDumper):

    def __init__(self,
                 filepath: str,
                 writer: AbstractSyncTempGraphWriter
                 ) -> None:
        self._filepath = filepath
        self._writer = writer

        logger.debug(
            f"Creating instance:"
            f"filepath: {filepath}"
            f"Writer: {writer}"
        )

    def dump_temp_graph(self,
                        temp_graph_df: pd.DataFrame
                        ) -> None:
        filepath = os.path.abspath(self._filepath)
        logger.debug(f"Storing temp graph to {filepath}")
        with open(filepath, mode="wb") as output_file:
            self._writer.write_temp_graph_to_binary_stream(output_file, temp_graph_df)
