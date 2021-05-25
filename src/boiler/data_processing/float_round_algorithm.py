import logging
import math

import numpy as np
import pandas as pd


class AbstractFloatRoundAlgorithm:

    def round_value(self, value: float) -> float:
        raise NotImplementedError

    def round_series(self, series: pd.Series) -> pd.Series:
        raise NotImplementedError


class ArithmeticFloatRoundAlgorithm(AbstractFloatRoundAlgorithm):

    def __init__(self, decimals: int = 0) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._decimals = decimals
        self._logger.debug(f"Decimals count is set to {decimals}")

    def round_value(self, value: float) -> float:
        # self._logger.debug(f"Rounding value: {value}")
        multiplier = 10 ** self._decimals
        rounded_abs = math.floor(abs(value) * multiplier + 0.5) / multiplier
        rounded_value = math.copysign(rounded_abs, value)
        # self._logger.debug(f"Rounded value: {rounded_value}")
        return rounded_value

    def round_series(self, series: pd.Series) -> pd.Series:
        # self._logger.debug("Rounding series")
        # noinspection PyTypeChecker
        multiplier = 10 ** self._decimals
        rounded_series = series.copy()
        rounded_series = rounded_series.abs() * multiplier + 0.5
        rounded_series = np.floor(rounded_series) / multiplier
        rounded_series = np.copysign(rounded_series, series)
        # self._logger.debug("Series is rounded")
        return rounded_series
