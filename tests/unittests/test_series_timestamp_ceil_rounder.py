import pytest

from boiler.data_processing.timestamp_round_algorithm import CeilTimestampRoundAlgorithm
from unittests.dataset_timestamp_round_testing import SeriesTimestampRoundTesting


class TestSeriesTimestampCeilRounder(SeriesTimestampRoundTesting):

    @pytest.fixture
    def round_algo(self):
        return CeilTimestampRoundAlgorithm(round_step=self.timedelta)
