import logging
from typing import Optional

import pandas as pd

from boiler.constants import column_names
from boiler.data_processing.timestamp_round_algorithm \
    import AbstractTimestampRoundAlgorithm


class AbstractTimestampInterpolationAlgorithm:

    def process_df(self,
                   df: pd.DataFrame,
                   required_min_timestamp: pd.Timestamp,
                   required_max_timestamp: pd.Timestamp) -> pd.DataFrame:
        raise NotImplementedError


class TimestampInterpolationAlgorithm(AbstractTimestampInterpolationAlgorithm):

    def __init__(self,
                 timestamp_round_algo: Optional[AbstractTimestampRoundAlgorithm] = None,
                 interpolation_step: Optional[pd.Timedelta] = None,
                 timestamp_column_name: str = column_names.TIMESTAMP) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._interpolation_step = interpolation_step
        self._timestamp_round_algo = timestamp_round_algo
        self._timestamp_column_name = timestamp_column_name

        self._logger.debug(f"Timestamp Round algo is {timestamp_round_algo}")
        self._logger.debug(f"Interpolation step is {interpolation_step}")
        self._logger.debug(f"Timestamp column name is {timestamp_column_name}")

    def set_interpolation_step(self, interpolation_step: pd.Timedelta) -> None:
        self._logger.debug(f"Interpolation step is set to {interpolation_step}")
        self._interpolation_step = interpolation_step

    def set_timestamp_round_algo(self, round_algo: AbstractTimestampRoundAlgorithm) -> None:
        self._logger.debug(f"Round algo is set to {round_algo}")
        self._timestamp_round_algo = round_algo

    def process_df(self,
                   df: pd.DataFrame,
                   required_min_timestamp: pd.Timestamp,
                   required_max_timestamp: pd.Timestamp) -> pd.DataFrame:
        self._logger.debug("Interpolating is requested")
        df = df.copy()
        if required_min_timestamp is not None:
            df = self._complementary_min_timestamp(df, required_min_timestamp)
        if required_max_timestamp is not None:
            df = self._complementary_max_timestamp(df, required_max_timestamp)
        df = df.sort_values(by=self._timestamp_column_name, ignore_index=True)
        df = self._interpolate_passes_of_timestamp(df)
        self._logger.debug("Interpolated")
        return df

    def _complementary_min_timestamp(self, df: pd.DataFrame, required_min_timestamp) -> pd.DataFrame:
        rounded_required_min_timestamp = self._timestamp_round_algo.round_value(required_min_timestamp)
        self._logger.debug(f"Complementary min timestamp to {rounded_required_min_timestamp} "
                           f"{[required_min_timestamp]}")

        df = df.copy()
        min_timestamp = df[self._timestamp_column_name].min()
        if min_timestamp > required_min_timestamp:
            new_df = pd.DataFrame(columns=df.columns)
            new_row = {
                self._timestamp_column_name: required_min_timestamp
            }
            new_df = new_df.append(new_row, ignore_index=True)
            df = new_df.append(df, ignore_index=True)

        return df

    def _complementary_max_timestamp(self, df: pd.DataFrame, required_max_timestamp) -> pd.DataFrame:
        rounded_required_max_timestamp = self._timestamp_round_algo.round_value(required_max_timestamp)
        self._logger.debug(f"Complementary max timestamp to"
                           f" {rounded_required_max_timestamp} {[required_max_timestamp]}")

        df = df.copy()
        max_timestamp = df[self._timestamp_column_name].max()
        if max_timestamp < required_max_timestamp:
            new_row = {
                self._timestamp_column_name: required_max_timestamp
            }
            df = df.append(new_row, ignore_index=True)

        return df

    def _interpolate_passes_of_timestamp(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.debug("Interpolating passes of datetime")

        timestamp_to_insert = []
        previous_timestamp = None
        for timestamp in df[self._timestamp_column_name].to_list():
            if previous_timestamp is None:
                previous_timestamp = timestamp
                continue
            next_timestamp = timestamp

            current_timestamp = previous_timestamp + self._interpolation_step
            while current_timestamp < next_timestamp:
                timestamp_to_insert.append({
                    self._timestamp_column_name: current_timestamp
                })
                current_timestamp += self._interpolation_step

            previous_timestamp = next_timestamp

        df = df.append(timestamp_to_insert, ignore_index=True)
        df = df.sort_values(by=self._timestamp_column_name, ignore_index=True)

        return df
