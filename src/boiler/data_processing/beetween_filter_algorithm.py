import logging
from typing import Union

import pandas as pd

from boiler.constants import column_names


class AbstractTimestampFilterAlgorithm:

    def filter_df_by_min_max_timestamp(self,
                                       df: pd.DataFrame,
                                       min_timestamp: Union[pd.Timestamp, None],
                                       max_timestamp: Union[pd.Timestamp, None]
                                       ) -> pd.DataFrame:
        raise NotImplementedError


class FullClosedTimestampFilterAlgorithm(AbstractTimestampFilterAlgorithm):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)

        self._timestamp_column_name = column_names.TIMESTAMP

    def filter_df_by_min_max_timestamp(self,
                                       df: pd.DataFrame,
                                       min_timestamp: Union[pd.Timestamp, None],
                                       max_timestamp: Union[pd.Timestamp, None]
                                       ) -> pd.DataFrame:
        self._logger.debug(f"Filter by range: [{min_timestamp}, {max_timestamp}]")
        if min_timestamp is not None:
            df = df[df[self._timestamp_column_name] >= min_timestamp]
        if max_timestamp is not None:
            df = df[df[self._timestamp_column_name] <= max_timestamp]
        df = df.copy()
        return df


class LeftClosedTimestampFilterAlgorithm(AbstractTimestampFilterAlgorithm):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)

        self._timestamp_column_name = column_names.TIMESTAMP

    def filter_df_by_min_max_timestamp(self,
                                       df: pd.DataFrame,
                                       min_timestamp: Union[pd.Timestamp, None],
                                       max_timestamp: Union[pd.Timestamp, None]
                                       ) -> pd.DataFrame:
        self._logger.debug(f"Filter range: [{min_timestamp}, {max_timestamp})")
        if min_timestamp is not None:
            df = df[df[self._timestamp_column_name] >= min_timestamp]
        if max_timestamp is not None:
            df = df[df[self._timestamp_column_name] < max_timestamp]
        df = df.copy()
        return df
