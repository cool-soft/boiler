import logging
import os
from typing import Optional

import pandas as pd

from boiler.temp_graph.io.sync.sync_temp_graph_dumper import SyncTempGraphDumper
from boiler.temp_graph.io.sync.sync_temp_graph_writer import SyncTempGraphWriter


class SyncTempGraphFileDumper(SyncTempGraphDumper):

    def __init__(self,
                 filepath: Optional[str] = None,
                 writer: Optional[SyncTempGraphWriter] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._filepath = filepath
        self._writer = writer

        self._logger.debug(f"Filepath is {filepath}")
        self._logger.debug(f"Writer is {writer}")

    def set_filepath(self, filepath: str) -> None:
        self._logger.debug(f"Filepath is set to {filepath}")
        self._filepath = filepath

    def set_writer(self, writer: SyncTempGraphWriter) -> None:
        self._logger.debug(f"Writer is set to {writer}")
        self._writer = writer

    def dump_temp_graph(self, temp_graph_df: pd.DataFrame) -> None:
        filepath = os.path.abspath(self._filepath)
        self._logger.debug(f"Storing temp graph to {filepath}")
        with open(filepath, mode="wb") as output_file:
            self._writer.write_temp_graph_to_binary_stream(output_file, temp_graph_df)
        self._logger.debug("Temp graph is stored")
