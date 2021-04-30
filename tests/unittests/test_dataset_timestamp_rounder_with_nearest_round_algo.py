import pandas as pd
import pytest

from boiler.data_processing.processing_algo.timestamp_round_algorithm import NearestTimestampRoundAlgorithm
from unittests.dataset_timestamp_round_testing import DatasetTimestampRoundTesting


class TestDatasetTimestampRounderWithNearestAlgo(DatasetTimestampRoundTesting):

    @pytest.fixture
    def alternative_round_algo(self):
        def alternative_algo(df: pd.DataFrame):
            df = df.copy()
            df[self.timestamp_column] = df[self.timestamp_column].dt.\
                round(f"{int(self.timedelta.total_seconds())}s")
            return df
        return alternative_algo

    @pytest.fixture
    def round_algo(self):
        return NearestTimestampRoundAlgorithm(round_step=self.timedelta)
