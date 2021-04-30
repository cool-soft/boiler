import io
import logging
from typing import BinaryIO

import pandas as pd

from boiler.constants import column_names
from boiler.time_delta.io.sync.abstract_sync_timedelta_reader import AbstractSyncTimedeltaReader


class SyncTimedeltaCSVReader(AbstractSyncTimedeltaReader):

    def __init__(self, encoding="utf-8") -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._encoding = encoding

        self._logger.debug(f"Encoding is {encoding}")

    def set_encoding(self, encoding: str) -> None:
        self._logger.debug(f"Encoding is set to {encoding}")
        self._encoding = encoding

    def read_timedelta_from_binary_stream(self, binary_stream: BinaryIO) -> pd.DataFrame:
        self._logger.debug("Loading timedelta")
        with io.TextIOWrapper(binary_stream, encoding=self._encoding) as text_stream:
            timedelta_df = pd.read_csv(text_stream)
        timedelta_df = self._convert_timedelta_from_seconds(timedelta_df)
        self._logger.debug("Timedelta is loaded")
        return timedelta_df

    def _convert_timedelta_from_seconds(self, timedelta_df: pd.DataFrame) -> pd.DataFrame:
        timedelta_df = timedelta_df.copy()
        timedelta_df[column_names.AVG_TIMEDELTA] = \
            timedelta_df[column_names.AVG_TIMEDELTA].apply(lambda seconds: pd.Timedelta(seconds=seconds))
        return timedelta_df
