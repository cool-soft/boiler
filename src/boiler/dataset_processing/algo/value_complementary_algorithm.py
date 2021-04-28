import logging

import pandas as pd

from boiler.constants import column_names


class AbstractValueComplementaryAlgorithm:

    def complementary_dataframe_column(self, df: pd.DataFrame, column_name: str) -> pd.DataFrame:
        raise NotImplementedError


class StartValueComplementaryAlgorithm(AbstractValueComplementaryAlgorithm):

    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

    def complementary_dataframe_column(self, df: pd.DataFrame, column_name: str) -> pd.DataFrame:
        self._logger.debug(f"Complementary column {column_name}")
        df = df.copy()
        first_datetime_index = df[column_names.TIMESTAMP].idxmin()
        first_valid_index = df[column_name].first_valid_index()
        if first_valid_index != first_datetime_index:
            first_valid_value = df.loc[first_valid_index, column_name]
            df.loc[first_datetime_index, column_name] = first_valid_value
        return df


class EndValueComplementaryAlgorithm(AbstractValueComplementaryAlgorithm):

    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

    def complementary_dataframe_column(self, df: pd.DataFrame, column_name: str) -> pd.DataFrame:
        self._logger.debug(f"Complementary column {column_name}")
        df = df.copy()
        last_datetime_index = df[column_names.TIMESTAMP].idxmax()
        last_valid_index = df[column_name].last_valid_index()
        if last_valid_index != last_datetime_index:
            last_valid_value = df.loc[last_valid_index, column_name]
            df.loc[last_datetime_index, column_name] = last_valid_value
        return df