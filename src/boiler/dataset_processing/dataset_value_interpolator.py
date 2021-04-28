import logging
from typing import List, Optional

import pandas as pd

from boiler.constants import column_names
from boiler.dataset_processing.algo.value_interpolation_algorithm import AbstractValueInterpolationAlgorithm
from boiler.dataset_processing.abstract_dataset_processor import AbstractDatasetProcessor


class DatasetValueInterpolator(AbstractDatasetProcessor):

    def __init__(self,
                 interpolation_algorithm: Optional[AbstractValueInterpolationAlgorithm] = None,
                 columns_to_interpolate: Optional[List[str]] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        if columns_to_interpolate is None:
            columns_to_interpolate = []
        self._columns_to_interpolate = columns_to_interpolate
        self._interpolation_algorithm = interpolation_algorithm

        self._logger.debug(f"Value interpolation algorithm is {interpolation_algorithm}")
        self._logger.debug(f"Columns to interpolate {columns_to_interpolate}")

    def set_interpolation_algorithm(self, algorithm: AbstractValueInterpolationAlgorithm):
        self._logger.debug(f"Algorithm is set to {algorithm}")
        self._interpolation_algorithm = algorithm

    def set_columns_to_interpolate(self, columns: List[str]) -> None:
        self._logger.debug("Columns to interpolate is set to ")
        self._columns_to_interpolate = columns

    def process_df(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.debug("Interpolating is requested")
        df = df.copy()
        df = df.sort_values(by=column_names.TIMESTAMP, ignore_index=True)
        for column_to_interpolate in self._columns_to_interpolate:
            df[column_to_interpolate] = self._interpolation_algorithm.interpolate_series(df[column_to_interpolate])
        self._logger.debug("Interpolated")
        return df
