from random import random, randint

import pandas as pd
import pytest
from dateutil.tz import tzlocal

from boiler.constants import column_names
from boiler.data_processing.dataset_processors.dataset_timestamp_rounder import DatasetTimestampRounder


class DatasetTimestampRoundTesting:

    timedelta = pd.Timedelta(minutes=10)

    random_timedelta_min_seconds = 10
    random_timedelta_max_seconds = int(timedelta.total_seconds())

    timestamp_column = column_names.TIMESTAMP
    other_columns = ["col_0", "col_1"]
    row_count = 25

    @pytest.fixture
    def dataset(self):
        df = pd.DataFrame(columns=[self.timestamp_column, *self.other_columns])
        current_timestamp = pd.Timestamp.now(tz=tzlocal())
        for i in range(self.row_count):
            new_row = {}
            for column_name in self.other_columns:
                new_row[column_name] = random()
            random_timedelta = pd.Timedelta(
                seconds=randint(
                    self.random_timedelta_min_seconds,
                    self.random_timedelta_max_seconds
                )
            )
            new_row[self.timestamp_column] = current_timestamp + random_timedelta
            df = df.append(new_row, ignore_index=True)
            current_timestamp += self.timedelta

        return df

    @pytest.fixture
    def processor(self, round_algo):
        return DatasetTimestampRounder(
            timestamp_column_name=self.timestamp_column,
            round_algo=round_algo
        )

    def test_dataset_timestamp_round_ceil(self, dataset, processor, alternative_round_algo):
        rounded_dataset = processor.process_df(dataset)
        alternative_processed_dataset = alternative_round_algo(dataset)

        for column in self.other_columns:
            assert column in rounded_dataset.columns

        assert rounded_dataset.to_dict("records") == alternative_processed_dataset.to_dict("records")
