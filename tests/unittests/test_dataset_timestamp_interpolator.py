from random import random

import pandas as pd
import pytest
from dateutil.tz import tzlocal

from boiler.constants import column_names
from boiler.data_processing.dataset_processors.dataset_timestamp_interpolator \
    import DatasetTimestampInterpolator
from boiler.data_processing.processing_algo.timestamp_round_algorithm import FloorTimestampRoundAlgorithm


class TestDatasetTimestampInterpolator:

    timestamp_column_name = column_names.TIMESTAMP
    columns = ["col_0", "col_1"]
    timedelta = pd.Timedelta(minutes=10)

    @pytest.fixture
    def dataset(self, timestamp_round_algorithm):
        current_timestamp = pd.Timestamp.now(tz=tzlocal())
        current_timestamp = timestamp_round_algorithm.round_value(current_timestamp)

        row_count = 100
        generation_prob = 0.3

        df = pd.DataFrame(columns=self.columns)

        for i in range(row_count//2-1):
            if random() <= generation_prob:
                df = self._append_row(df, current_timestamp)
            current_timestamp += self.timedelta

        df = self._append_row(df, current_timestamp)
        current_timestamp += self.timedelta

        for i in range(row_count//2-1):
            if random() <= generation_prob:
                df = self._append_row(df, current_timestamp)
            current_timestamp += self.timedelta

        return df

    # noinspection PyMethodMayBeStatic
    def _append_row(self, df, current_timestamp):
        new_row = {}
        for column_name in self.columns:
            new_row[column_name] = random()
        new_row[self.timestamp_column_name] = current_timestamp
        df = df.append(new_row, ignore_index=True)
        return df

    @pytest.fixture
    def timestamp_round_algorithm(self):
        return FloorTimestampRoundAlgorithm(round_step=self.timedelta)

    @pytest.fixture
    def processor(self, timestamp_round_algorithm):
        return DatasetTimestampInterpolator(
            timestamp_round_algo=timestamp_round_algorithm,
            interpolation_step=self.timedelta,
            timestamp_column_name=self.timestamp_column_name
        )

    def test_dataset_timestamp_interpolation(self, dataset, processor, timestamp_round_algorithm):

        start_timestamp = dataset[self.timestamp_column_name].min()
        start_timestamp -= 5 * self.timedelta
        end_timestamp = dataset[self.timestamp_column_name].max()
        end_timestamp += 5 * self.timedelta

        processor.set_start_timestamp(start_timestamp)
        processor.set_end_timestamp(end_timestamp)
        interpolated_dataset = processor.process_df(dataset)

        assert interpolated_dataset[self.timestamp_column_name].min() == start_timestamp
        assert interpolated_dataset[self.timestamp_column_name].max() == end_timestamp

        timestamp_list = interpolated_dataset[self.timestamp_column_name].to_list()
        for i in range(0, len(timestamp_list)-1):
            assert timestamp_list[i] + self.timedelta == timestamp_list[i+1]

        nan_count = len(interpolated_dataset) - len(dataset)
        for column_name in self.columns:
            assert interpolated_dataset[column_name].isna().sum() == nan_count
