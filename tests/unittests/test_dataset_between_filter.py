from random import random, randint, randrange

import pandas as pd
import pytest
from dateutil.tz import tzlocal

from boiler.constants import column_names
from boiler.data_processing.beetween_filter_algorithm import \
    FullClosedTimestampFilterAlgorithm, LeftClosedTimestampFilterAlgorithm


class TestDatasetBetweenFilter:

    other_columns = ["col_1", "col_2"]
    min_generated_timestamp = pd.Timestamp.now(tz=tzlocal())
    max_generated_timestamp = min_generated_timestamp + pd.Timedelta(seconds=10000)
    min_filter_val = min_generated_timestamp + pd.Timedelta(seconds=100)
    max_filter_val = min_generated_timestamp + pd.Timedelta(seconds=1000)
    time_step = pd.Timedelta(seconds=10)

    @pytest.fixture
    def full_closed_filter(self):
        return FullClosedTimestampFilterAlgorithm()

    @pytest.fixture
    def left_closed_filter(self):
        return LeftClosedTimestampFilterAlgorithm()

    @pytest.fixture
    def dataset(self):
        df = pd.DataFrame(columns=[column_names.TIMESTAMP, *self.other_columns])
        current_timestamp = self.min_generated_timestamp
        while current_timestamp <= self.max_generated_timestamp:
            row = {}
            for column_name in self.other_columns:
                row[column_name] = random()
            row[column_names.TIMESTAMP] = current_timestamp
            df = df.append(row, ignore_index=True)
            current_timestamp += self.time_step

        return df

    def test_full_closed_between_filter(self, dataset, full_closed_filter):
        filtered_dataset = full_closed_filter.filter_df_by_min_max_timestamp(
            dataset,
            self.min_filter_val,
            self.max_filter_val
        )

        assert filtered_dataset[column_names.TIMESTAMP].min() == self.min_filter_val
        assert filtered_dataset[column_names.TIMESTAMP].max() == self.max_filter_val

        for column_name in self.other_columns:
            assert column_name in filtered_dataset.columns

    def test_left_closed_between_filter(self, dataset, left_closed_filter):
        filtered_dataset = left_closed_filter.filter_df_by_min_max_timestamp(
            dataset,
            self.min_filter_val,
            self.max_filter_val
        )

        assert filtered_dataset[column_names.TIMESTAMP].min() == self.min_filter_val
        assert filtered_dataset[column_names.TIMESTAMP].max() <= self.max_filter_val

        for column_name in self.other_columns:
            assert column_name in filtered_dataset.columns
