import logging
from typing import Any, Optional

import pandas as pd

from boiler.data_processing.dataset_processors.abstract_dataset_processor import AbstractDatasetProcessor
from boiler.data_processing.processing_algo.beetween_filter_algorithm import AbstractBetweenFilterAlgorithm


class DatasetMinMaxFilter(AbstractDatasetProcessor):

    def __init__(self,
                 filter_algorithm: Optional[AbstractBetweenFilterAlgorithm] = None,
                 min_value: Optional[Any] = None,
                 max_value: Optional[Any] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._filter_algorithm = filter_algorithm
        self._min_value = min_value
        self._max_value = max_value

        self._logger.debug(f"Filter algorithm {filter_algorithm}")
        self._logger.debug(f"Min value is {min_value}")
        self._logger.debug(f"Max value is {max_value}")

    def set_filter_algorithm(self, algorithm: AbstractBetweenFilterAlgorithm) -> None:
        self._logger.debug(f"Filter algorithm is set to {algorithm}")
        self._filter_algorithm = algorithm

    def set_min_value(self, min_value: Any) -> None:
        self._logger.debug(f"Min value is set to {min_value}")
        self._min_value = min_value

    def set_max_value(self, max_value: Any) -> None:
        self._logger.debug(f"Max value is set to {max_value}")
        self._max_value = max_value

    def process_df(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.debug("Processing is requested")
        df = self._filter_algorithm.filter_df_by_min_max_values(df, self._min_value, self._max_value)
        self._logger.debug("Processed")
        return df
