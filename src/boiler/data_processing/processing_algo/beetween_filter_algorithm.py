import logging
from typing import Any

import pandas as pd

from boiler.constants import column_names


class AbstractBetweenFilterAlgorithm:

    def filter_df_by_min_max_values(self,
                                    df: pd.DataFrame,
                                    min_value: Any,
                                    max_value: Any) -> pd.DataFrame:
        raise NotImplementedError


class FullClosedBetweenFilterAlgorithm(AbstractBetweenFilterAlgorithm):

    def __init__(self,
                 column_name: str = column_names.TIMESTAMP) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._column_name = column_name

        self._logger.debug(f"Column is {column_name}")

    def set_column_to_filter_by(self, column_name: str) -> None:
        self._logger.debug(f"Column to filter by is set to {column_name}")
        self._column_name = column_name

    def filter_df_by_min_max_values(self,
                                    df: pd.DataFrame,
                                    min_value: Any = None,
                                    max_value: Any = None) -> pd.DataFrame:
        self._logger.debug(f"Filter by column {self._column_name}: [{min_value}, {max_value}]")
        if min_value is not None:
            df = df[df[self._column_name] >= min_value]
        if max_value is not None:
            df = df[df[self._column_name] <= max_value]
        df = df.copy()
        return df


class LeftClosedBetweenFilterAlgorithm(AbstractBetweenFilterAlgorithm):

    def __init__(self,
                 column_name: str = column_names.TIMESTAMP) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._column_name = column_name

        self._logger.debug(f"Column is {column_name}")

    def set_column_to_filter_by(self, column_name: str) -> None:
        self._logger.debug(f"Column to filter by is set to {column_name}")
        self._column_name = column_name

    def filter_df_by_min_max_values(self,
                                    df: pd.DataFrame,
                                    min_value: Any = None,
                                    max_value: Any = None) -> pd.DataFrame:
        self._logger.debug(f"Filter by column {self._column_name}: [{min_value}, {max_value})")
        if min_value is not None:
            df = df[df[self._column_name] >= min_value]
        if max_value is not None:
            df = df[df[self._column_name] < max_value]
        df = df.copy()
        return df
