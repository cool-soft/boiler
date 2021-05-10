from random import random

import pandas as pd
import pytest

from boiler.data_processing.value_interpolation_algorithm import \
    LinearOutsideValueInterpolationAlgorithm


class TestSeriesValueOutboundLinearInterpolator:

    start_invalid_index_count = 3
    central_invalid_index_start = 5
    central_invalid_index_end = 10
    end_invalid_index_start = central_invalid_index_end + 5
    end_invalid_index_count = 3

    @pytest.fixture
    def series_to_interpolate(self):
        data = []

        for i in range(self.start_invalid_index_count):
            data.append(None)

        for i in range(self.central_invalid_index_start - self.start_invalid_index_count):
            data.append(random())

        for i in range(self.central_invalid_index_end - self.central_invalid_index_start):
            data.append(None)

        for i in range(self.end_invalid_index_start - self.central_invalid_index_end):
            data.append(random())

        for i in range(self.end_invalid_index_count):
            data.append(None)

        return pd.Series(data)

    @pytest.fixture
    def algorithm(self):
        return LinearOutsideValueInterpolationAlgorithm()

    def test_series_linear_value_interpolation(self, series_to_interpolate, algorithm):
        interpolated_series = algorithm.interpolate_series(series_to_interpolate)

        assert interpolated_series[:self.start_invalid_index_count].isnull().sum() == 0
        assert interpolated_series[self.start_invalid_index_count:self.central_invalid_index_start].isnull().sum() == 0
        assert interpolated_series[self.central_invalid_index_start:self.central_invalid_index_end].isnull().sum() \
               == self.central_invalid_index_end - self.central_invalid_index_start
        assert interpolated_series[self.central_invalid_index_end:self.end_invalid_index_start].isnull().sum() == 0
        assert interpolated_series[self.end_invalid_index_start:].isnull().sum() == 0
