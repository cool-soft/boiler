import logging

import pandas as pd

from boiler.constants import column_names
from boiler.data_processing.dataset_processors.abstract_dataset_processor import AbstractDatasetProcessor


class DatasetSortProcessor(AbstractDatasetProcessor):

    def __init__(self,
                 column_name: str = column_names.TIMESTAMP,
                 ascending: bool = True) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._column_to_sort_by = column_name
        self._ascending = ascending

        self._logger.debug(f"Column to sort by {column_name}")
        self._logger.debug(f"Ascending is {ascending}")

    def set_column_to_sort_by(self, column_name: str) -> None:
        self._logger.debug(f"Column to sort by is set to {column_name}")
        self._column_to_sort_by = column_name

    def set_ascending(self, ascending: bool) -> None:
        self._logger.debug(f"Ascending is set to{ascending}")
        self._ascending = ascending

    def process_df(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.debug("Sorting by timestamp is requested")
        df = df.sort_values(by=self._column_to_sort_by, ignore_index=True, ascending=self._ascending)
        self._logger.debug("Sorted")
        return df
