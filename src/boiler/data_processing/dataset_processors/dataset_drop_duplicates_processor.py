import logging

import pandas as pd

from boiler.constants import column_names
from boiler.data_processing.dataset_processors.abstract_dataset_processor import AbstractDatasetProcessor


class DatasetTimestampRounder(AbstractDatasetProcessor):

    def __init__(self,
                 column_by: str = column_names.TIMESTAMP,
                 keep_mode: str = "first") -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._column_by = column_by
        self._keep_mode = keep_mode

        self._logger.debug(f"Column by is {column_by}")
        self._logger.debug(f"Keep mode is {keep_mode}")

    def set_column_by(self, column_name: str) -> None:
        self._logger.debug(f"Column by is set to {column_name}")
        self._column_by = column_name

    def set_keep_mode(self, keep_mode: str) -> None:
        self._logger.debug(f"Keep mode is set to {keep_mode}")
        self._keep_mode = keep_mode

    def process_df(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.debug("Processing is requested")
        df = df.copy()
        df = df.drop_duplicates(self._column_by, keep=self._keep_mode, ignore_index=True)
        self._logger.debug("Processed")
        return df
