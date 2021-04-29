from random import random

import pandas as pd
import pytest
from dateutil.tz import tzlocal

from boiler.constants import column_names
from boiler.data_processing.dataset_processors.dataset_timestamp_interpolator \
    import DatasetTimestampInterpolator
from boiler.data_processing.processing_algo.timestamp_round_algorithm import FloorTimestampRoundAlgorithm


class TestDatasetTimestampInterpolator:

    @pytest.fixture
    def timestamp_column_name(self):
        return column_names.TIMESTAMP

    @pytest.fixture
    def columns(self):
        return ["col_0", "col_1"]

    @pytest.fixture
    def timedelta(self):
        return pd.Timedelta(minutes=10)

    @pytest.fixture
    def dataset(self, timedelta, columns, timestamp_column_name, timestamp_round_algorithm):
        current_timestamp = pd.Timestamp.now(tz=tzlocal())
        current_timestamp = timestamp_round_algorithm.round_value(current_timestamp)

        row_count = 100
        generation_prob = 0.3

        df = pd.DataFrame(columns=columns)

        for i in range(row_count//2-1):
            if random() <= generation_prob:
                df = self._append_row(df, columns, timestamp_column_name, current_timestamp)
            current_timestamp += timedelta

        df = self._append_row(df, columns, timestamp_column_name, current_timestamp)
        current_timestamp += timedelta

        for i in range(row_count//2-1):
            if random() <= generation_prob:
                df = self._append_row(df, columns, timestamp_column_name, current_timestamp)
            current_timestamp += timedelta

        return df

    # noinspection PyMethodMayBeStatic
    def _append_row(self, df, columns, timestamp_column_name, current_timestamp):
        new_row = {}
        for column_name in columns:
            new_row[column_name] = random()
        new_row[timestamp_column_name] = current_timestamp
        df = df.append(new_row, ignore_index=True)
        return df

    @pytest.fixture
    def timestamp_round_algorithm(self, timedelta):
        return FloorTimestampRoundAlgorithm(round_step=timedelta)

    @pytest.fixture
    def processor(self, timestamp_round_algorithm, timedelta, timestamp_column_name):
        return DatasetTimestampInterpolator(
            timestamp_round_algo=timestamp_round_algorithm,
            interpolation_step=timedelta,
            timestamp_column_name=timestamp_column_name
        )

    def test_dataset_timestamp_interpolation(self,
                                             dataset,
                                             processor,
                                             timestamp_column_name,
                                             timestamp_round_algorithm,
                                             timedelta,
                                             columns):

        start_timestamp = dataset[timestamp_column_name].min()
        start_timestamp -= 5 * timedelta
        end_timestamp = dataset[timestamp_column_name].max()
        end_timestamp += 5 * timedelta

        processor.set_start_timestamp(start_timestamp)
        processor.set_end_timestamp(end_timestamp)
        interpolated_dataset = processor.process_df(dataset)

        assert interpolated_dataset[timestamp_column_name].min() == start_timestamp
        assert interpolated_dataset[timestamp_column_name].max() == end_timestamp

        timestamp_list = interpolated_dataset[timestamp_column_name].to_list()
        for i in range(0, len(timestamp_list)-1):
            assert timestamp_list[i] + timedelta == timestamp_list[i+1]

        nan_count = len(interpolated_dataset) - len(dataset)
        for column_name in columns:
            assert interpolated_dataset[column_name].isna().sum() == nan_count
