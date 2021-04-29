import pandas as pd
import pytest

from boiler.data_processing.dataset_processors.dataset_drop_duplicates_processor import DatasetDropDuplicatesProcessor


class TestDatasetDropDuplicates:

    duplicates_column = "col_0"
    not_duplicates_column = "col_1"
    value_first = 10
    value_second = 20
    value_last = 30
    unique_count = 20

    @pytest.fixture
    def dataset(self):
        df = pd.DataFrame(columns=[self.duplicates_column, self.not_duplicates_column])
        for i in range(self.unique_count):
            df = df.append(
                {self.duplicates_column: i, self.not_duplicates_column: self.value_first},
                ignore_index=True
            )
            df = df.append(
                {self.duplicates_column: i, self.not_duplicates_column: self.value_second},
                ignore_index=True
            )
            df = df.append(
                {self.duplicates_column: i, self.not_duplicates_column: self.value_last},
                ignore_index=True
            )

        return df

    @pytest.fixture
    def processor(self):
        return DatasetDropDuplicatesProcessor(column_by=self.duplicates_column)

    def test_dataset_drop_duplicates_processor_keep_first(self, dataset, processor):
        processor.set_keep_mode("first")
        processed_dataset = processor.process_df(dataset)
        assert len(processed_dataset) == self.unique_count
        assert (processed_dataset[self.not_duplicates_column] == self.value_first).all()

    def test_dataset_drop_duplicates_processor_keep_last(self, dataset, processor):
        processor.set_keep_mode("last")
        processed_dataset = processor.process_df(dataset)
        assert len(processed_dataset) == self.unique_count
        assert (processed_dataset[self.not_duplicates_column] == self.value_last).all()
