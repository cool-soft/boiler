import logging
import os
from typing import Optional

import pandas as pd

from boiler.time_delta.io.sync.abstract_sync_timedelta_loader import AbstractSyncTimedeltaLoader
from boiler.time_delta.io.sync.abstract_sync_timedelta_reader import AbstractSyncTimedeltaReader


class SyncTimedeltaFileLoader(AbstractSyncTimedeltaLoader):

    def __init__(self,
                 filepath: Optional[str] = None,
                 reader: Optional[AbstractSyncTimedeltaReader] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._filepath = filepath
        self._reader = reader

        self._logger.debug(f"Filepath is {filepath}")
        self._logger.debug(f"Reader is {reader}")

    def set_reader(self, reader: AbstractSyncTimedeltaReader):
        self._logger.debug(f"Reader is set to {reader}")
        self._reader = reader

    def load_timedelta(self) -> pd.DataFrame:
        filepath = os.path.abspath(self._filepath)
        self._logger.debug(f"Loading timedelta from {filepath}")
        with open(filepath, mode="rb") as input_file:
            timedelta_df = self._reader.read_timedelta_from_binary_stream(input_file)
        self._logger.debug("Timedelta is loaded")
        return timedelta_df
