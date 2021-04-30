from random import random

import pandas as pd
import pytest

from boiler.data_processing.beetween_filter_algorithm import \
    FullClosedBetweenFilterAlgorithm, LeftClosedBetweenFilterAlgorithm


class TestDatasetBetweenFilter:

    column_name_to_filter = "col_0"
    other_columns = ["col_1", "col_2"]
    min_val = -100
    max_val = 100
    min_filter_val = -50
    max_filter_val = 50

    @pytest.fixture
    def full_closed_filter(self):
        return FullClosedBetweenFilterAlgorithm(column_name=self.column_name_to_filter)

    @pytest.fixture
    def left_closed_filter(self):
        return LeftClosedBetweenFilterAlgorithm(column_name=self.column_name_to_filter)

    @pytest.fixture
    def dataset(self):
        df = pd.DataFrame(columns=[self.column_name_to_filter, *self.other_columns])
        for i in range(self.min_val, self.max_val):
            row = {}
            for column_name in self.other_columns:
                row[column_name] = random()
            row[self.column_name_to_filter] = i
            df = df.append(row, ignore_index=True)
        return df

    def test_full_closed_between_filter(self, dataset, full_closed_filter):
        filtered_dataset = full_closed_filter.filter_df_by_min_max_values(
            dataset,
            self.min_filter_val,
            self.max_filter_val
        )

        assert filtered_dataset[self.column_name_to_filter].min() == self.min_filter_val
        assert filtered_dataset[self.column_name_to_filter].max() == self.max_filter_val

        for column_name in self.other_columns:
            assert column_name in filtered_dataset.columns

    def test_left_closed_between_filter(self, dataset, left_closed_filter):
        filtered_dataset = left_closed_filter.filter_df_by_min_max_values(
            dataset,
            self.min_filter_val,
            self.max_filter_val
        )

        assert filtered_dataset[self.column_name_to_filter].min() == self.min_filter_val
        assert filtered_dataset[self.column_name_to_filter].max() <= self.max_filter_val

        for column_name in self.other_columns:
            assert column_name in filtered_dataset.columns
