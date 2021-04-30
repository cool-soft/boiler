import logging
import os
from typing import Optional

import pandas as pd

from boiler.time_delta.io.sync.abstract_sync_timedelta_dumper import AbstractSyncTimedeltaDumper
from boiler.time_delta.io.sync.abstract_sync_timedelta_writer import AbstractSyncTimedeltaWriter


class SyncTimedeltaFileDumper(AbstractSyncTimedeltaDumper):

    def __init__(self,
                 filepath: Optional[str] = None,
                 writer: Optional[AbstractSyncTimedeltaWriter] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._filepath = filepath
        self._writer = writer

        self._logger.debug(f"Filepath is {filepath}")
        self._logger.debug(f"Writer is {writer}")

    def set_filepath(self, filepath: str) -> None:
        self._logger.debug(f"Filepath is set to {filepath}")
        self._filepath = filepath

    def set_writer(self, writer: AbstractSyncTimedeltaWriter) -> None:
        self._logger.debug(f"Writer is set to {writer}")
        self._writer = writer

    def dump_timedelta(self, timedelta_df: pd.DataFrame) -> None:
        filepath = os.path.abspath(self._filepath)
        self._logger.debug(f"Storing timedelta to {filepath}")
        with open(filepath, mode="wb") as output_file:
            self._writer.write_timedelta_to_binary_stream(output_file, timedelta_df)
        self._logger.debug("Timedelta is stored")
