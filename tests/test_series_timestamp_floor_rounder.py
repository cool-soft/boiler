import pytest

from boiler.data_processing.timestamp_round_algorithm import FloorTimestampRoundAlgorithm
from dataset_timestamp_round_testing import SeriesTimestampRoundTesting


class TestSeriesTimestampFloorRounder(SeriesTimestampRoundTesting):

    @pytest.fixture
    def round_algo(self):
        return FloorTimestampRoundAlgorithm(round_step=self.timedelta)
