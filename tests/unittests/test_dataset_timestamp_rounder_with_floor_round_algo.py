import pytest
import pandas as pd

from boiler.data_processing.processing_algo.timestamp_round_algorithm import FloorTimestampRoundAlgorithm
from unittests.dataset_timestamp_round_testing import DatasetTimestampRoundTesting


class TestDatasetTimestampRounderWithFloorAlgo(DatasetTimestampRoundTesting):

    @pytest.fixture
    def round_algo(self):
        return FloorTimestampRoundAlgorithm(round_step=self.timedelta)

    @pytest.fixture
    def alternative_round_algo(self):
        def alternative_algo(df: pd.DataFrame):
            df = df.copy()
            df[self.timestamp_column] = df[self.timestamp_column].dt.\
                floor(f"{int(self.timedelta.total_seconds())}s")
            return df
        return alternative_algo
