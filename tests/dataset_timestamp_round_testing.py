from random import randint

import pandas as pd
import pytest
from dateutil.tz import tzlocal


class SeriesTimestampRoundTesting:

    timedelta = pd.Timedelta(minutes=10)
    random_timedelta_min_seconds = 10
    random_timedelta_max_seconds = int(timedelta.total_seconds())
    row_count = 25

    @pytest.fixture
    def alternative_round_algo(self, round_algo):
        def alternative_algo(series: pd.Series):
            # noinspection PyTypeChecker
            return series.apply(round_algo.round_value)
        return alternative_algo

    @pytest.fixture
    def series_to_round(self):
        data = []
        current_timestamp = pd.Timestamp.now(tz=tzlocal())
        for i in range(self.row_count):
            random_timedelta = pd.Timedelta(
                seconds=randint(
                    self.random_timedelta_min_seconds,
                    self.random_timedelta_max_seconds
                )
            )
            data.append(current_timestamp + random_timedelta)
            current_timestamp += self.timedelta

        return pd.Series(data)

    def test_series_timestamp_round(self, series_to_round, round_algo, alternative_round_algo):
        rounded_series = round_algo.round_series(series_to_round)
        alternative_processed_series = alternative_round_algo(series_to_round)
        # noinspection PyUnresolvedReferences
        assert (rounded_series == alternative_processed_series).all()
