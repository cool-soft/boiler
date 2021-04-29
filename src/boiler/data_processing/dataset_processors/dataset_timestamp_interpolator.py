import logging
from typing import Optional

import pandas as pd

from boiler.constants import column_names
from boiler.data_processing.dataset_processors.abstract_dataset_processor import AbstractDatasetProcessor
from boiler.data_processing.processing_algo.timestamp_round_algorithm import AbstractTimestampRoundAlgorithm


class DatasetTimestampInterpolator(AbstractDatasetProcessor):

    def __init__(self,
                 timestamp_round_algo: Optional[AbstractTimestampRoundAlgorithm] = None,
                 interpolation_step: Optional[pd.Timedelta] = None,
                 start_timestamp: Optional[pd.Timestamp] = None,
                 end_timestamp: Optional[pd.Timestamp] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._interpolation_step = interpolation_step
        self._timestamp_round_algo = timestamp_round_algo
        self._start_timestamp = start_timestamp
        self._end_timestamp = end_timestamp

        self._logger.debug(f"Timestamp Round algo is {timestamp_round_algo}")
        self._logger.debug(f"Interpolation step is {interpolation_step}")
        self._logger.debug(f"Start timestamp is {start_timestamp}")
        self._logger.debug(f"End timestamp is {end_timestamp}")

    def set_start_datetime(self, timestamp: pd.Timestamp) -> None:
        self._logger.debug(f"Start timestamp is set to {timestamp}")
        self._start_timestamp = timestamp

    def set_end_datetime(self, timestamp: pd.Timestamp) -> None:
        self._logger.debug(f"End timestamp is set to {timestamp}")
        self._end_timestamp = timestamp

    def set_interpolation_step(self, interpolation_step: pd.Timedelta) -> None:
        self._logger.debug(f"Interpolation step is set to {interpolation_step}")
        self._interpolation_step = interpolation_step

    def set_timestamp_round_algo(self, round_algo: AbstractTimestampRoundAlgorithm) -> None:
        self._logger.debug(f"Round algo is set to {round_algo}")
        self._timestamp_round_algo = round_algo

    def process_df(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.debug("Interpolating is requested")
        df = df.copy()
        df = self._complementary_start_timestamp(df)
        df = self._complementary_end_timestamp(df)
        df = df.sort_values(by=column_names.TIMESTAMP, ignore_index=True)
        df = self._interpolate_passes_of_timestamp(df)
        self._logger.debug("Interpolated")
        return df

    def _complementary_start_timestamp(self, df: pd.DataFrame) -> pd.DataFrame:
        start_timestamp = self._timestamp_round_algo.round_value(self._start_timestamp)
        self._logger.debug(f"Complementary start timestamp to value {start_timestamp}")
        df = df.copy()
        first_datetime_idx = df[column_names.TIMESTAMP].idxmin()
        first_datetime = df.loc[first_datetime_idx, column_names.TIMESTAMP]
        if first_datetime > start_timestamp:
            # TODO: вставлять в начало
            df = df.append({column_names.TIMESTAMP: start_timestamp}, ignore_index=True)
        return df

    def _complementary_end_timestamp(self, df: pd.DataFrame) -> pd.DataFrame:
        end_timestamp = self._timestamp_round_algo.round_value(self._end_timestamp)
        self._logger.debug(f"Complementary end timestamp to value {end_timestamp}")
        df = df.copy()
        last_datetime_idx = df[column_names.TIMESTAMP].idxmax()
        last_datetime = df.loc[last_datetime_idx, column_names.TIMESTAMP]
        if last_datetime < end_timestamp:
            df = df.append({column_names.TIMESTAMP: end_timestamp}, ignore_index=True)
        return df

    def _interpolate_passes_of_timestamp(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.debug("Interpolating passes of datetime")

        timestamp_to_insert = []
        previous_timestamp = None
        for timestamp in df[column_names.TIMESTAMP].to_list():
            if previous_timestamp is None:
                previous_timestamp = timestamp
                continue
            next_timestamp = timestamp

            current_timestamp = previous_timestamp + self._interpolation_step
            while current_timestamp < next_timestamp:
                timestamp_to_insert.append({
                    column_names.TIMESTAMP: current_timestamp
                })
                current_timestamp += self._interpolation_step

            previous_timestamp = next_timestamp

        df = df.append(timestamp_to_insert, ignore_index=True)
        df = df.sort_values(by=column_names.TIMESTAMP, ignore_index=True)

        return df
