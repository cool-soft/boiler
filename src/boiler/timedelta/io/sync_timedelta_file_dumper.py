import os

import pandas as pd
from boiler.logger import logger

from boiler.timedelta.io.abstract_sync_timedelta_dumper import AbstractSyncTimedeltaDumper
from boiler.timedelta.io.abstract_sync_timedelta_writer import AbstractSyncTimedeltaWriter


class SyncTimedeltaFileDumper(AbstractSyncTimedeltaDumper):

    def __init__(self,
                 filepath: str,
                 writer: AbstractSyncTimedeltaWriter
                 ) -> None:
        self._filepath = filepath
        self._writer = writer

        logger.debug(
            f"Creating instance:"
            f"filepath: {filepath}"
            f"writer: {writer}"
        )

    def dump_timedelta(self, timedelta_df: pd.DataFrame) -> None:
        filepath = os.path.abspath(self._filepath)
        logger.debug(f"Storing timedelta to {filepath}")
        with open(filepath, mode="wb") as output_file:
            self._writer.write_timedelta_to_binary_stream(output_file, timedelta_df)
