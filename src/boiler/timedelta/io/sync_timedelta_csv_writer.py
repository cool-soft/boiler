import io
from typing import BinaryIO

import pandas as pd
from boiler.logging import logger

from boiler.constants import column_names
from boiler.timedelta.io.abstract_sync_timedelta_writer import AbstractSyncTimedeltaWriter


class SyncTimedeltaCSVWriter(AbstractSyncTimedeltaWriter):

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

    def write_timedelta_to_binary_stream(self,
                                         binary_stream: BinaryIO,
                                         timedelta_df: pd.DataFrame) -> None:
        logger.debug("Storing timedelta")
        timedelta_df = self._convert_timedelta_to_seconds(timedelta_df)
        with io.TextIOWrapper(binary_stream, encoding=self._encoding) as text_stream:
            timedelta_df.to_csv(text_stream, index=False, sep=self._separator)

    # noinspection PyMethodMayBeStatic
    def _convert_timedelta_to_seconds(self, timedelta_df: pd.DataFrame) -> pd.DataFrame:
        timedelta_df = timedelta_df.copy()
        timedelta_df[column_names.AVG_TIMEDELTA] = \
            timedelta_df[column_names.AVG_TIMEDELTA].dt.total_seconds()
        return timedelta_df
