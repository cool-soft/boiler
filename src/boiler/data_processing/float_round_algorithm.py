import logging
import math

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
        self._logger.debug(f"Rounding value: {value}")
        rounded_value = self._round(value)
        self._logger.debug(f"Rounded value: {rounded_value}")
        return rounded_value

    def round_series(self, series: pd.Series) -> pd.Series:
        self._logger.debug(f"Rounding series")
        # noinspection PyTypeChecker
        rounded_series = series.apply(lambda x: self._round(x))
        self._logger.debug(f"Series is rounded")
        return rounded_series

    def _round(self, n):
        multiplier = 10 ** self._decimals
        rounded_abs = math.floor(abs(n) * multiplier + 0.5) / multiplier
        return math.copysign(rounded_abs, n)
