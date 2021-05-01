import logging
from typing import Optional

import pandas as pd


class AbstractOverdriveAlgorithm:

    def apply_overdrive_to_series(self, series: pd.Series) -> pd.Series:
        raise NotImplementedError


class MinMaxOverdriveAlgorithm(AbstractOverdriveAlgorithm):

    def __init__(self,
                 min_value: Optional[float] = 0,
                 max_value: Optional[float] = 100) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._min_value = min_value
        self._max_value = max_value

        self._logger.debug(f"Min value is {min_value}")
        self._logger.debug(f"Max value is {max_value}")

    def set_min_value(self, value: float) -> None:
        self._logger.debug(f"Min value is set to {value}")
        self._min_value = value

    def set_max_value(self, value: float) -> None:
        self._logger.debug(f"Max value is set to {value}")
        self._max_value = value

    def apply_overdrive_to_series(self, series: pd.Series) -> pd.Series:
        self._logger.debug(f"Apply overdrive [{self._min_value}, {self._max_value}]")
        series = series.copy()
        series.loc[series > self._max_value] = self._max_value
        series.loc[series < self._min_value] = self._min_value
        return series
