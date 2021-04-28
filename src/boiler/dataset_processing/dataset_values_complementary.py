import logging
from typing import Optional, List

import pandas as pd

from boiler.dataset_processing.dataset_processor import DatasetProcessor
from boiler.dataset_processing.algo.value_complementary_algorithm import AbstractValueComplementaryAlgorithm


class DatasetValuesComplementary(DatasetProcessor):

    def __init__(self,
                 columns_to_complementary: Optional[List[str]] = None,
                 complementary_algorithm: Optional[AbstractValueComplementaryAlgorithm] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance of the provider")

        if columns_to_complementary is None:
            self._columns_to_complementary = []
        self._columns_to_complementary = columns_to_complementary
        self._complementary_algo = complementary_algorithm

        self._logger.debug(f"Columns to complementary are {columns_to_complementary}")
        self._logger.debug(f"Complementary algo is {complementary_algorithm}")

    def set_complementary_algo(self, complementary_algo: AbstractValueComplementaryAlgorithm) -> None:
        self._logger.debug(f"Round algo is set to {complementary_algo}")
        self._complementary_algo = complementary_algo

    def process_df(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.debug("Interpolating is requested")
        df = df.copy()
        for column_name in self._columns_to_complementary:
            df = self._complementary_algo.complementary_dataframe_column(df, column_name)
        self._logger.debug("Interpolated")
        return df
