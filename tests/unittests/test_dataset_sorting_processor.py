from random import random

import pandas as pd
import pytest

from boiler.data_processing.dataset_processors.dataset_sort_processor import DatasetSortProcessor


class TestDatasetSortingProcessor:

    columns = ["col_0", "col_1", "col_2"]
    row_count = 20

    @pytest.fixture
    def dataset(self):
        df = pd.DataFrame(columns=self.columns)
        for i in range(self.row_count):
            new_row = {}
            for column_name in df.columns:
                new_row[column_name] = random()
            df = df.append(new_row, ignore_index=True)

        return df

    @pytest.fixture
    def processor(self):
        return DatasetSortProcessor()

    def test_dataset_sort_processor_ascending_true_first_column(self, dataset, processor):
        column_to_sort_by = self.columns[0]
        processor.set_column_to_sort_by(column_to_sort_by)
        processor.set_ascending(True)
        sorted_dataset = processor.process_df(dataset)
        sorted_column = sorted_dataset[column_to_sort_by].to_list()
        for i in range(1, len(sorted_dataset)):
            assert sorted_column[i - 1] <= sorted_column[i]

    def test_dataset_sort_processor_ascending_false_second_column(self, dataset, processor):

        column_to_sort_by = self.columns[1]
        processor.set_column_to_sort_by(column_to_sort_by)
        processor.set_ascending(False)
        sorted_dataset = processor.process_df(dataset)
        sorted_column = sorted_dataset[column_to_sort_by].to_list()
        for i in range(1, len(sorted_dataset)):
            assert sorted_column[i - 1] >= sorted_column[i]
