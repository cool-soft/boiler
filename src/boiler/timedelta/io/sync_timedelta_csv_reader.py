import io
from typing import BinaryIO

import pandas as pd
from boiler.logger import boiler_logger

from boiler.constants import column_names
from boiler.timedelta.io.abstract_sync_timedelta_reader import AbstractSyncTimedeltaReader


class SyncTimedeltaCSVReader(AbstractSyncTimedeltaReader):

    def __init__(self,
                 encoding: str = "utf-8",
                 separator: str = ";"
                 ) -> None:
        self._encoding = encoding
        self._separator = separator

        boiler_logger.debug(
            f"Creating instance:"
            f"encoding: {encoding}"
            f"separator: {separator}"
        )

    def read_timedelta_from_binary_stream(self,
                                          binary_stream: BinaryIO
                                          ) -> pd.DataFrame:
        boiler_logger.debug("Loading timedelta")
        with io.TextIOWrapper(binary_stream, encoding=self._encoding) as text_stream:
            timedelta_df = pd.read_csv(text_stream, sep=self._separator)
        timedelta_df = self._convert_timedelta_from_seconds(timedelta_df)
        return timedelta_df

    # noinspection PyMethodMayBeStatic
    def _convert_timedelta_from_seconds(self,
                                        timedelta_df: pd.DataFrame
                                        ) -> pd.DataFrame:
        timedelta_df = timedelta_df.copy()
        timedelta_df[column_names.AVG_TIMEDELTA] = \
            timedelta_df[column_names.AVG_TIMEDELTA].apply(lambda seconds: pd.Timedelta(seconds=seconds))
        return timedelta_df
