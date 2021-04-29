import logging
from typing import Optional

import pandas as pd

from boiler.constants import column_names
from boiler.data_processing.dataset_processors.abstract_dataset_processor import AbstractDatasetProcessor
from boiler.data_processing.processing_algo.timestamp_round_algorithm import AbstractTimestampRoundAlgorithm


class DatasetTimestampRounder(AbstractDatasetProcessor):

    def __init__(self, round_algo: Optional[AbstractTimestampRoundAlgorithm] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._round_algo = round_algo

        self._logger.debug(f"Round algo is {round_algo}")

    def set_round_algo(self, round_algo: AbstractTimestampRoundAlgorithm) -> None:
        self._logger.debug(f"Round algo is set to {round_algo}")
        self._round_algo = round_algo

    def process_df(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.debug("Processing is requested")
        df = df.copy()
        df[column_names.TIMESTAMP] = self._round_algo.round_series(df[column_names.TIMESTAMP])
        # TODO: вынести в отдельный процессор
        df = df.drop_duplicates(column_names.TIMESTAMP, ignore_index=True)
        self._logger.debug("Processed")
        return df
