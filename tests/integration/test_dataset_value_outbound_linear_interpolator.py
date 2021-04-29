from random import random

import pandas as pd
import pytest

from boiler.data_processing.dataset_processors.dataset_value_interpolator import DatasetValueInterpolator
from boiler.data_processing.processing_algo.value_interpolation_algorithm import \
    LinearOutsideValueInterpolationAlgorithm


class TestDatasetValueOutboundLinearInterpolator:

    @pytest.fixture
    def start_invalid_index_count(self):
        return 3

    @pytest.fixture
    def central_invalid_index_start(self):
        return 5

    @pytest.fixture
    def central_invalid_index_end(self):
        return 10

    @pytest.fixture
    def end_invalid_index_start(self, central_invalid_index_end):
        return central_invalid_index_end + 5

    @pytest.fixture
    def end_invalid_index_count(self):
        return 3

    @pytest.fixture
    def columns_to_interpolate(self):
        return ["col_0", "col_1", "col_2"]

    @pytest.fixture
    def columns_not_interpolate(self):
        return ["col_3", "col_4"]

    @pytest.fixture
    def columns(self, columns_to_interpolate, columns_not_interpolate):
        return [*columns_to_interpolate, *columns_not_interpolate]

    @pytest.fixture
    def dataset(self,
                columns,
                start_invalid_index_count,
                central_invalid_index_start,
                central_invalid_index_end,
                end_invalid_index_start,
                end_invalid_index_count):

        df = pd.DataFrame(columns=columns)

        for i in range(start_invalid_index_count):
            new_row = {}
            for column_name in columns:
                new_row[column_name] = None
            df = df.append(new_row, ignore_index=True)

        for i in range(central_invalid_index_start - start_invalid_index_count):
            new_row = {}
            for column_name in columns:
                new_row[column_name] = random()
            df = df.append(new_row, ignore_index=True)

        for i in range(central_invalid_index_end - central_invalid_index_start):
            new_row = {}
            for column_name in columns:
                new_row[column_name] = None
            df = df.append(new_row, ignore_index=True)

        for i in range(end_invalid_index_start - central_invalid_index_end):
            new_row = {}
            for column_name in columns:
                new_row[column_name] = random()
            df = df.append(new_row, ignore_index=True)

        for i in range(end_invalid_index_count):
            new_row = {}
            for column_name in columns:
                new_row[column_name] = None
            df = df.append(new_row, ignore_index=True)

        return df

    @pytest.fixture
    def algorithm(self):
        return LinearOutsideValueInterpolationAlgorithm()

    @pytest.fixture
    def processor(self, algorithm, columns_to_interpolate):
        return DatasetValueInterpolator(
            algorithm,
            columns_to_interpolate
        )

    def test_dataset_linear_value_interpolation(self,
                                                dataset,
                                                columns_to_interpolate,
                                                columns_not_interpolate,
                                                processor,
                                                start_invalid_index_count,
                                                central_invalid_index_start,
                                                central_invalid_index_end,
                                                end_invalid_index_start,
                                                end_invalid_index_count):
        nan_count_before = {}
        for column in columns_not_interpolate:
            nan_count_before[column] = dataset[column].isnull().sum()

        interpolated_dataset = processor.process_df(dataset)

        for column_name in columns_to_interpolate:
            column = interpolated_dataset[column_name]
            assert column[:start_invalid_index_count].isnull().sum() == 0
            assert column[start_invalid_index_count:central_invalid_index_start].isnull().sum() == 0
            assert column[central_invalid_index_start:central_invalid_index_end].isnull().sum() \
                   == central_invalid_index_end - central_invalid_index_start
            assert column[central_invalid_index_end:end_invalid_index_start].isnull().sum() == 0
            assert column[end_invalid_index_start:].isnull().sum() == 0

        for column in columns_not_interpolate:
            assert interpolated_dataset[column].isnull().sum() == nan_count_before.get(column)
