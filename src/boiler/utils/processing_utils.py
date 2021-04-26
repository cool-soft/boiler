import logging
import math
from enum import Enum
from typing import Optional

import numpy as np
import pandas as pd

from boiler.constants import column_names


def average_values(x: np.array, window_len: int = 4, window: str = 'hanning') -> np.array:
    if x.ndim != 1:
        raise ValueError("smooth only accepts 1 dimension arrays.")

    if x.size < window_len:
        raise ValueError("Input vector needs to be bigger than window size.")

    if window not in ['flat', 'hanning', 'hamming', 'bartlett', 'blackman']:
        raise ValueError("Window is on of 'flat', 'hanning', 'hamming', 'bartlett', 'blackman'")

    if window_len < 3:
        return x

    s = np.r_[x[window_len - 1:0:-1], x, x[-2:-window_len - 1:-1]]

    if window == 'flat':
        w = np.ones(window_len, 'd')
    else:
        w = getattr(np, window)(window_len)

    y = np.convolve(w / w.sum(), s, mode='valid')
    return y[(window_len // 2 - 1 + (window_len % 2)):-(window_len // 2)]
    # return y


def filter_by_timestamp_closed(df: pd.DataFrame,
                               start_datetime: Optional[pd.Timestamp] = None,
                               end_datetime: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    if start_datetime is not None:
        df = df[df[column_names.TIMESTAMP] >= start_datetime]
    if end_datetime is not None:
        df = df[df[column_names.TIMESTAMP] <= end_datetime]
    return df


class TimestampRoundAlgo:

    class RoundMode(Enum):
        FLOOR = 0
        CEIL = 1
        NEAREST = 2

    def __init__(self,
                 round_mode: RoundMode = RoundMode.CEIL,
                 round_step: Optional[pd.Timedelta] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._round_mode = round_mode
        self._round_step = round_step

        self._logger.debug(f"Round mode is {round_mode}")
        self._logger.debug(f"Round step is {round_step}")

    def set_round_mode(self, round_mode: RoundMode) -> None:
        self._logger.debug(f"Round mode is set to {round_mode}")
        self._round_mode = round_mode

    def set_round_step(self, round_step: pd.Timedelta) -> None:
        self._logger.debug(f"Round step is set to {round_step}")
        self._round_step = round_step

    def round_value(self, value: pd.Timestamp) -> pd.Timestamp:
        self._logger.debug(f"Rounding value: {value}")

        round_step_in_seconds = int(self._round_step.total_seconds())
        if self._round_mode is self.RoundMode.CEIL:
            rounded_value = value.ceil(f"{round_step_in_seconds}s")
        elif self._round_mode is self.RoundMode.FLOOR:
            rounded_value = value.floor(f"{round_step_in_seconds}s")
        elif self._round_mode is self.RoundMode.NEAREST:
            rounded_value = value.round(f"{round_step_in_seconds}s")
        else:
            raise ValueError(f"Incorrect round mode {self._round_mode}")

        self._logger.debug(f"Rounded value: {rounded_value}")
        return rounded_value

    def round_series(self, series: pd.Series) -> pd.Series:
        self._logger.debug(f"Rounding series")

        round_step_in_seconds = int(self._round_step.total_seconds())
        if self._round_mode is self.RoundMode.CEIL:
            rounded_series = series.dt.ceil(f"{round_step_in_seconds}s")
        elif self._round_mode is self.RoundMode.FLOOR:
            rounded_series = series.dt.floor(f"{round_step_in_seconds}s")
        elif self._round_mode is self.RoundMode.NEAREST:
            rounded_series = series.dt.round(f"{round_step_in_seconds}s")
        else:
            raise ValueError(f"Incorrect round mode {self._round_mode}")

        self._logger.debug(f"Series is rounded")
        return rounded_series


def arithmetic_round(number: float) -> int:
    number_floor = math.floor(number)
    if number - number_floor < 0.5:
        rounded_number = number_floor
    else:
        rounded_number = number_floor + 1
    return rounded_number