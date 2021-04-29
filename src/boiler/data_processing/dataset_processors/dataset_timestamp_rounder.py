import logging
from typing import Optional

import pandas as pd

from boiler.constants import column_names
from boiler.data_processing.dataset_processors.abstract_dataset_processor import AbstractDatasetProcessor
from boiler.data_processing.processing_algo.timestamp_round_algorithm import AbstractRoundAlgorithm


class DatasetTimestampRounder(AbstractDatasetProcessor):

    def __init__(self,
                 round_algo: Optional[AbstractRoundAlgorithm] = None,
                 timestamp_column_name: str = column_names.TIMESTAMP) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._round_algo = round_algo
        self._need_round_column_name = timestamp_column_name

        self._logger.debug(f"Round algo is {round_algo}")
        self._logger.debug(f"Need round column is {timestamp_column_name}")

    def set_round_algo(self, round_algo: AbstractRoundAlgorithm) -> None:
        self._logger.debug(f"Round algo is set to {round_algo}")
        self._round_algo = round_algo

    def set_need_round_column_name(self, column_name: str) -> None:
        self._logger.debug(f"Timestamp column name is set to {column_name}")
        self._need_round_column_name = column_name

    def process_df(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.debug("Processing is requested")
        df = df.copy()
        df[self._need_round_column_name] = self._round_algo.round_series(df[self._need_round_column_name])
        self._logger.debug("Processed")
        return df
