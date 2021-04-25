import logging
import os
from typing import Optional

import pandas as pd

from .sync_temp_graph_binary_writer import SyncTempGraphBinaryWriter
from .sync_temp_graph_dumper import SyncTempGraphDumper


class SyncTempGraphBinaryFileDumper(SyncTempGraphDumper):

    def __init__(self,
                 filepath: Optional[str] = None,
                 writer: Optional[SyncTempGraphBinaryWriter] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._filepath = filepath
        self._writer = writer

        self._logger.debug(f"Filepath is {filepath}")
        self._logger.debug(f"Writer is {writer}")

    def set_filepath(self, filepath: str) -> None:
        self._logger.debug(f"Filepath is set to {filepath}")
        self._filepath = filepath

    def set_writer(self, writer: SyncTempGraphBinaryWriter) -> None:
        self._logger.debug(f"Writer is set to {writer}")
        self._writer = writer

    def dump_temp_graph(self, weather_df: pd.DataFrame) -> None:
        filepath = os.path.abspath(self._filepath)
        self._logger.debug(f"Storing temp graph to {filepath}")
        with open(filepath, "wb") as binary_file:
            self._writer.write_temp_graph_to_binary_io(binary_file, weather_df)
        self._logger.debug("Temp graph is stored")
