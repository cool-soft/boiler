import logging
from typing import Optional

import pandas as pd

from boiler.dataset_processing.abstract_dataset_processor import AbstractDatasetProcessor
from boiler.dataset_processing.algo.timestamp_complementary_algorithm import AbstractTimestampComplementaryAlgorithm
from boiler.dataset_processing.algo.timestamp_round_algorithm import AbstractTimestampRoundAlgorithm


class DatasetTimestampComplementary(AbstractDatasetProcessor):

    def __init__(self,
                 round_algo: Optional[AbstractTimestampRoundAlgorithm] = None,
                 complementary_algorithm: Optional[AbstractTimestampComplementaryAlgorithm] = None,
                 required_timestamp: Optional[pd.Timestamp] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance of the provider")

        self._complementary_algo = complementary_algorithm
        self._required_timestamp = required_timestamp

        self._logger.debug(f"Complementary algo {complementary_algorithm}")
        self._logger.debug(f"Required timestamp  is {required_timestamp}")

    def set_required_timestamp(self, timestamp: pd.Timestamp):
        self._logger.debug(f"Required timestamp is set to {timestamp}")
        self._required_timestamp = timestamp

    def set_complementary_algo(self, complementary_algo: AbstractTimestampComplementaryAlgorithm) -> None:
        self._logger.debug(f"Round algo is set to {complementary_algo}")
        self._complementary_algo = complementary_algo

    def process_df(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.debug("Interpolating is requested")
        df = df.copy()
        required_timestamp = self._required_timestamp
        required_timestamp = self._timestamp_round_algo.round_value(required_timestamp)
        df = self._complementary_algo.complementary_timestamp(df, required_timestamp)
        self._logger.debug("Interpolated")
        return df
