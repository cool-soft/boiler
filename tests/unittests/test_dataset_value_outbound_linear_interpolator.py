from random import random

import pandas as pd
import pytest

from boiler.data_processing.dataset_processors.dataset_value_interpolator import DatasetValueInterpolator
from boiler.data_processing.processing_algo.value_interpolation_algorithm import \
    LinearOutsideValueInterpolationAlgorithm


class TestDatasetValueOutboundLinearInterpolator:

    start_invalid_index_count = 3
    central_invalid_index_start = 5
    central_invalid_index_end = 10
    end_invalid_index_start = central_invalid_index_end + 5
    end_invalid_index_count = 3
    columns_to_interpolate = ["col_0", "col_1", "col_2"]
    columns_not_interpolate = ["col_3", "col_4"]
    columns = [*columns_to_interpolate, *columns_not_interpolate]

    @pytest.fixture
    def dataset(self):

        df = pd.DataFrame(columns=self.columns)

        for i in range(self.start_invalid_index_count):
            new_row = {}
            for column_name in self.columns:
                new_row[column_name] = None
            df = df.append(new_row, ignore_index=True)

        for i in range(self.central_invalid_index_start - self.start_invalid_index_count):
            new_row = {}
            for column_name in self.columns:
                new_row[column_name] = random()
            df = df.append(new_row, ignore_index=True)

        for i in range(self.central_invalid_index_end - self.central_invalid_index_start):
            new_row = {}
            for column_name in self.columns:
                new_row[column_name] = None
            df = df.append(new_row, ignore_index=True)

        for i in range(self.end_invalid_index_start - self.central_invalid_index_end):
            new_row = {}
            for column_name in self.columns:
                new_row[column_name] = random()
            df = df.append(new_row, ignore_index=True)

        for i in range(self.end_invalid_index_count):
            new_row = {}
            for column_name in self.columns:
                new_row[column_name] = None
            df = df.append(new_row, ignore_index=True)

        return df

    @pytest.fixture
    def algorithm(self):
        return LinearOutsideValueInterpolationAlgorithm()

    @pytest.fixture
    def processor(self, algorithm):
        return DatasetValueInterpolator(
            algorithm,
            self.columns_to_interpolate
        )

    def test_dataset_linear_value_interpolation(self, dataset, processor):
        nan_count_before = {}
        for column in self.columns_not_interpolate:
            nan_count_before[column] = dataset[column].isnull().sum()

        interpolated_dataset = processor.process_df(dataset)

        for column_name in self.columns_to_interpolate:
            column = interpolated_dataset[column_name]
            assert column[:self.start_invalid_index_count].isnull().sum() == 0
            assert column[self.start_invalid_index_count:self.central_invalid_index_start].isnull().sum() == 0
            assert column[self.central_invalid_index_start:self.central_invalid_index_end].isnull().sum() \
                   == self.central_invalid_index_end - self.central_invalid_index_start
            assert column[self.central_invalid_index_end:self.end_invalid_index_start].isnull().sum() == 0
            assert column[self.end_invalid_index_start:].isnull().sum() == 0

        for column in self.columns_not_interpolate:
            assert interpolated_dataset[column].isnull().sum() == nan_count_before.get(column)
