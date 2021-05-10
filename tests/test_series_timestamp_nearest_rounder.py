import pytest

from boiler.data_processing.timestamp_round_algorithm import NearestTimestampRoundAlgorithm
from dataset_timestamp_round_testing import SeriesTimestampRoundTesting


class TestSeriesTimestampNearestRounder(SeriesTimestampRoundTesting):

    @pytest.fixture
    def round_algo(self):
        return NearestTimestampRoundAlgorithm(round_step=self.timedelta)
