import math

import numpy as np
import pandas as pd
from boiler.logger import logger


class AbstractFloatRoundAlgorithm:

    def round_value(self, value: float) -> float:
        raise NotImplementedError

    def round_series(self, series: pd.Series) -> pd.Series:
        raise NotImplementedError


class ArithmeticFloatRoundAlgorithm(AbstractFloatRoundAlgorithm):

    def __init__(self, decimals: int = 0) -> None:
        self._decimals = decimals
        logger.debug(
            f"Creating instance:"
            f"decimals: {decimals}"
        )

    def round_value(self, value: float) -> float:
        logger.debug(f"Rounding value: {value}")
        multiplier = 10 ** self._decimals
        rounded_abs = math.floor(abs(value) * multiplier + 0.5) / multiplier
        rounded_value = math.copysign(rounded_abs, value)
        return rounded_value

    def round_series(self, series: pd.Series) -> pd.Series:
        logger.debug(f"Rounding series; len: {len(series)}")
        multiplier = 10 ** self._decimals
        rounded_series = series.copy()
        rounded_series = rounded_series.abs() * multiplier + 0.5
        rounded_series = np.floor(rounded_series) / multiplier
        rounded_series = np.copysign(rounded_series, series)
        return rounded_series
