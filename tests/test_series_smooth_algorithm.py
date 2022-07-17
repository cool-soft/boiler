import numpy as np
import pandas as pd
import pytest

from boiler.data_processing.smooth_algorithm import NumpyBasedSmoothAlgorithm


class TestSeriesSmoothAlgorithm:

    min_generated_value = -100
    max_generated_value = 200

    point_count = 1000
    generation_step = 0.1

    @pytest.fixture
    def smooth_algorithm(self):
        return NumpyBasedSmoothAlgorithm()

    @pytest.fixture
    def series(self):
        x = np.arange(0, self.point_count/self.generation_step, self.generation_step)
        array = np.sin(x)
        array *= (self.max_generated_value - self.min_generated_value)/2
        array += (self.max_generated_value + self.min_generated_value)/2
        return pd.Series(array)

    def test_smooth_algorithm(self, series, smooth_algorithm):
        smooth_series = smooth_algorithm.smooth_series(series)
        assert isinstance(smooth_series, pd.Series)
        assert len(smooth_series) == len(series)
