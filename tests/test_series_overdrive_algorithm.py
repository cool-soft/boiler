import numpy as np
import pandas as pd
import pytest

from boiler.data_processing.overdrive_algorithm import MinMaxOverdriveAlgorithm


class TestSeriesOverdriveAlgorithm:

    min_generated_value = -100
    max_generated_value = 200

    min_filter_value = -50
    max_filter_value = 70

    point_count = 1000
    generation_step = 0.1

    @pytest.fixture
    def overdrive_algorithm(self):
        return MinMaxOverdriveAlgorithm(self.max_filter_value, self.max_filter_value)

    @pytest.fixture
    def series(self):
        x = np.arange(0, self.point_count/self.generation_step, self.generation_step)
        array = np.sin(x)
        array *= (self.max_generated_value - self.min_generated_value)/2
        array += (self.max_generated_value + self.min_generated_value)/2
        return pd.Series(array)

    def test_overdrive_algorithm(self, series, overdrive_algorithm):
        overdrive_series = overdrive_algorithm.apply_overdrive_to_series(series)

        assert overdrive_series.max() <= self.max_filter_value
        assert overdrive_series.min() >= self.min_filter_value
