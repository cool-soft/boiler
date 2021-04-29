from random import random

import pytest
import pandas as pd

from boiler.data_processing.processing_algo.value_interpolation_algorithm import LinearInsideValueInterpolationAlgorithm
from boiler.data_processing.dataset_processors.dataset_value_interpolator import DatasetValueInterpolator


class TestDatasetValueInboundLinearInterpolator:

    columns_to_interpolate = ["col_0", "col_1", "col_2"]
    columns_not_interpolate = ["col_3", "col_4"]
    columns = [*columns_to_interpolate, *columns_not_interpolate]

    @pytest.fixture
    def dataset(self):
        row_count = 100
        generation_prob = 0.3

        df = pd.DataFrame(columns=self.columns)

        row_without_nones = {}
        for column_name in self.columns:
            row_without_nones[column_name] = random()
        df = df.append(row_without_nones, ignore_index=True)

        for i in range(row_count):
            new_row = {}
            for column_name in self.columns:
                if random() <= generation_prob:
                    new_row[column_name] = random()
                else:
                    new_row[column_name] = None
            df = df.append(new_row, ignore_index=True)

        df = df.append(row_without_nones, ignore_index=True)

        return df

    @pytest.fixture
    def algorithm(self):
        return LinearInsideValueInterpolationAlgorithm()

    @pytest.fixture
    def processor(self, algorithm):
        return DatasetValueInterpolator(
            interpolation_algorithm=algorithm,
            columns_to_interpolate=self.columns_to_interpolate
        )

    def test_dataset_linear_value_interpolation(self, dataset, processor):
        nan_count_before = {}
        for column in self.columns_not_interpolate:
            nan_count_before[column] = dataset[column].isnull().sum()

        interpolated_dataset = processor.process_df(dataset)

        for column in self.columns_to_interpolate:
            assert interpolated_dataset[column].isnull().sum() == 0
        
        for column in self.columns_not_interpolate:
            assert interpolated_dataset[column].isnull().sum() == nan_count_before.get(column)
