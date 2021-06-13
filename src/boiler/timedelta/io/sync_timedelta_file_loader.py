import os

import pandas as pd
from boiler.logger import boiler_logger

from boiler.timedelta.io.abstract_sync_timedelta_loader import AbstractSyncTimedeltaLoader
from boiler.timedelta.io.abstract_sync_timedelta_reader import AbstractSyncTimedeltaReader


class SyncTimedeltaFileLoader(AbstractSyncTimedeltaLoader):

    def __init__(self,
                 filepath: str,
                 reader: AbstractSyncTimedeltaReader
                 ) -> None:
        self._filepath = filepath
        self._reader = reader

        boiler_logger.debug(
            f"Creating instance:"
            f"filepath: {filepath}"
            f"reader: {reader}"
        )

    def load_timedelta(self) -> pd.DataFrame:
        filepath = os.path.abspath(self._filepath)
        boiler_logger.debug(f"Loading timedelta from {filepath}")
        with open(filepath, mode="rb") as input_file:
            timedelta_df = self._reader.read_timedelta_from_binary_stream(input_file)
        return timedelta_df
