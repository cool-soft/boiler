from typing import Optional

import pandas as pd
from boiler.logging import logger


class AbstractOverdriveAlgorithm:

    def apply_overdrive_to_series(self, series: pd.Series) -> pd.Series:
        raise NotImplementedError


class MinMaxOverdriveAlgorithm(AbstractOverdriveAlgorithm):

    def __init__(self,
                 min_value: Optional[float] = 0,
                 max_value: Optional[float] = 100
                 ) -> None:
        self._min_value = min_value
        self._max_value = max_value

        logger.debug(
            f"Creating instance"
            f"min value is {min_value}"
            f"max value is {max_value}"
        )

    def apply_overdrive_to_series(self, series: pd.Series) -> pd.Series:
        logger.debug(f"Apply overdrive to series; len={len(series)}")
        series = series.copy()
        series.loc[series > self._max_value] = self._max_value
        series.loc[series < self._min_value] = self._min_value
        return series
