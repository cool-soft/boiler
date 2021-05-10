from random import random

import pytest
import pandas as pd

from boiler.data_processing.value_interpolation_algorithm import LinearInsideValueInterpolationAlgorithm


class TestSeriesValueInboundLinearInterpolator:

    row_count = 100
    generation_prob = 0.3

    @pytest.fixture
    def series_to_interpolate(self):
        data = [random()]

        for i in range(self.row_count):
            if random() <= self.generation_prob:
                data.append(random())
            else:
                data.append(None)

        data = [random()]

        return pd.Series(data)

    @pytest.fixture
    def algorithm(self):
        return LinearInsideValueInterpolationAlgorithm()

    def test_series_linear_value_interpolation(self, series_to_interpolate, algorithm):
        interpolated_series = algorithm.interpolate_series(series_to_interpolate)
        # noinspection PyArgumentList
        assert interpolated_series.isnull().sum() == 0
