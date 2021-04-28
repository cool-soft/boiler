import logging

import pandas as pd

from boiler.constants import column_names
from boiler.dataset_processing.abstract_dataset_processor import AbstractDatasetProcessor


class DatasetValueInterpolator(AbstractDatasetProcessor):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

    def process_df(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.debug("Sorting by timestamp is requested")
        df = df.copy()
        df = df.sort_values(by=column_names.TIMESTAMP, ignore_index=True)
        self._logger.debug("Sorted")
        return df
