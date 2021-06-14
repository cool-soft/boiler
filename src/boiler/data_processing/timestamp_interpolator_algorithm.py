from typing import Union

import pandas as pd
from boiler.logger import logger

from boiler.constants import column_names
from boiler.data_processing.timestamp_round_algorithm import AbstractTimestampRoundAlgorithm


class AbstractTimestampInterpolationAlgorithm:

    def process_df(self,
                   df: pd.DataFrame,
                   min_required_timestamp: Union[pd.Timestamp, None],
                   max_required_timestamp: Union[pd.Timestamp, None]
                   ) -> pd.DataFrame:
        raise NotImplementedError


class TimestampInterpolationAlgorithm(AbstractTimestampInterpolationAlgorithm):

    def __init__(self,
                 timestamp_round_algo: AbstractTimestampRoundAlgorithm,
                 interpolation_step: pd.Timedelta,
                 ) -> None:
        self._interpolation_step = interpolation_step
        self._timestamp_round_algo = timestamp_round_algo
        self._timestamp_column_name = column_names.TIMESTAMP

        logger.debug(
            f"Creating instance:"
            f"timestamp round algo: {timestamp_round_algo}"
            f"interpolation step: {interpolation_step}"
        )

    def process_df(self,
                   df: pd.DataFrame,
                   min_required_timestamp: Union[pd.Timestamp, None],
                   max_required_timestamp: Union[pd.Timestamp, None]
                   ) -> pd.DataFrame:
        logger.debug(
            f"Interpolating for [{min_required_timestamp}, {max_required_timestamp}]; "
            f"df len = {len(df)}"
        )
        df = df.copy()
        if min_required_timestamp is not None:
            df = self._complementary_min_timestamp(df, min_required_timestamp)
        if min_required_timestamp is not None:
            df = self._complementary_max_timestamp(df, max_required_timestamp)
        df = df.sort_values(by=self._timestamp_column_name, ignore_index=True)
        df = self._interpolate_passes_of_timestamp(df)
        return df

    def _complementary_min_timestamp(self,
                                     df: pd.DataFrame,
                                     required_min_timestamp: pd.Timestamp
                                     ) -> pd.DataFrame:
        rounded_required_min_timestamp = self._timestamp_round_algo.round_value(required_min_timestamp)

        df = df.copy()
        min_timestamp = df[self._timestamp_column_name].min()
        if min_timestamp > rounded_required_min_timestamp:
            new_df = pd.DataFrame(
                columns=df.columns,
                data=[
                    {self._timestamp_column_name: rounded_required_min_timestamp},
                ]
            )
            df = df.append(
                new_df,
                ignore_index=True
            )

        return df

    def _complementary_max_timestamp(self,
                                     df: pd.DataFrame,
                                     required_max_timestamp: pd.Timestamp
                                     ) -> pd.DataFrame:
        rounded_required_max_timestamp = self._timestamp_round_algo.round_value(required_max_timestamp)

        df = df.copy()
        max_timestamp = df[self._timestamp_column_name].max()
        if max_timestamp < rounded_required_max_timestamp:
            new_df = pd.DataFrame(
                columns=df.columns,
                data=[
                    {self._timestamp_column_name: rounded_required_max_timestamp},
                ]
            )
            df = df.append(new_df, ignore_index=True)

        return df

    def _interpolate_passes_of_timestamp(self,
                                         df: pd.DataFrame
                                         ) -> pd.DataFrame:
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
