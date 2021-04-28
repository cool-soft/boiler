import logging

import pandas as pd

from boiler.constants import column_names


class AbstractTimestampComplementaryAlgorithm:

    def complementary_timestamp(self, df: pd.DataFrame, required_timestamp: pd.Timestamp) -> pd.DataFrame:
        raise NotImplementedError


class StartTimestampComplementaryAlgorithm(AbstractTimestampComplementaryAlgorithm):

    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

    def complementary_timestamp(self, df: pd.DataFrame, required_timestamp: pd.Timestamp) -> pd.DataFrame:
        self._logger.debug(f"Complementary timestamp to value {required_timestamp}")
        df = df.copy()
        first_datetime_idx = df[column_names.TIMESTAMP].idxmin()
        first_datetime = df.loc[first_datetime_idx, column_names.TIMESTAMP]
        if first_datetime > required_timestamp:
            # TODO: вставлять в начало
            df = df.append({column_names.TIMESTAMP: required_timestamp}, ignore_index=True)
            df = df.sort_values(by=column_names.TIMESTAMP, ignore_index=True)
        return df


class EndTimestampComplementaryAlgorithm(AbstractTimestampComplementaryAlgorithm):

    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

    def complementary_timestamp(self, df: pd.DataFrame, required_timestamp: pd.Timestamp) -> pd.DataFrame:
        self._logger.debug(f"Complementary timestamp to value {required_timestamp}")
        df = df.copy()
        last_datetime_idx = df[column_names.TIMESTAMP].idxmax()
        last_datetime = df.loc[last_datetime_idx, column_names.TIMESTAMP]
        if last_datetime < required_timestamp:
            df = df.append({column_names.TIMESTAMP: required_timestamp}, ignore_index=True)
        return df