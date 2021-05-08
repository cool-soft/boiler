import pandas as pd
import pytest

from boiler.data_processing.float_round_algorithm import ArithmeticFloatRoundAlgorithm


class TestArithmeticFloatRoundAlgorithm:
    TO_ROUND_COLUMN = "to_round"
    CHECK_COLUMN = "check"

    DECIMALS = 0

    @pytest.fixture
    def dataset(self):
        data = [
            {self.TO_ROUND_COLUMN: -3.5, self.CHECK_COLUMN: -4.0},
            {self.TO_ROUND_COLUMN: -3.49, self.CHECK_COLUMN: -3.0},
            {self.TO_ROUND_COLUMN: -2.5, self.CHECK_COLUMN: -3.0},
            {self.TO_ROUND_COLUMN: -2.49, self.CHECK_COLUMN: -2.0},
            {self.TO_ROUND_COLUMN: -1.5, self.CHECK_COLUMN: -2.0},
            {self.TO_ROUND_COLUMN: -1.49, self.CHECK_COLUMN: -1.0},
            {self.TO_ROUND_COLUMN: -1.0, self.CHECK_COLUMN: -1.0},
            {self.TO_ROUND_COLUMN: -0.5, self.CHECK_COLUMN: -1.0},
            {self.TO_ROUND_COLUMN: -0.49, self.CHECK_COLUMN: 0.0},
            {self.TO_ROUND_COLUMN: 0.49, self.CHECK_COLUMN: 0.0},
            {self.TO_ROUND_COLUMN: 0.5, self.CHECK_COLUMN: 1.0},
            {self.TO_ROUND_COLUMN: 1.49, self.CHECK_COLUMN: 1.0},
            {self.TO_ROUND_COLUMN: 2.49, self.CHECK_COLUMN: 2.0},
            {self.TO_ROUND_COLUMN: 2.5, self.CHECK_COLUMN: 3.0},
            {self.TO_ROUND_COLUMN: 3.49, self.CHECK_COLUMN: 3.0},
            {self.TO_ROUND_COLUMN: 3.5, self.CHECK_COLUMN: 4.0},
        ]
        return pd.DataFrame(data)

    @pytest.fixture
    def round_algo(self):
        return ArithmeticFloatRoundAlgorithm(decimals=self.DECIMALS)

    def test_round_series(self, dataset, round_algo):
        rounded_series = round_algo.round_series(dataset[self.TO_ROUND_COLUMN])
        assert (rounded_series == dataset[self.CHECK_COLUMN]).all()

    def test_round_values(self, dataset, round_algo):
        data_to_round = dataset[self.TO_ROUND_COLUMN].to_list()
        data_to_check = dataset[self.CHECK_COLUMN].to_list()

        for to_round, to_check in zip(data_to_round, data_to_check):
            assert round_algo.round_value(to_round) == to_check
