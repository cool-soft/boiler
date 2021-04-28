import logging
from typing import Optional

import pandas as pd


class AbstractTimestampRoundAlgorithm:

    def round_value(self, value: pd.Timestamp) -> pd.Timestamp:
        raise NotImplementedError

    def round_series(self, series: pd.Series) -> pd.Series:
        raise NotImplementedError


class FloorTimestampRoundAlgorithm(AbstractTimestampRoundAlgorithm):

    def __init__(self, round_step: Optional[pd.Timedelta] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._round_step = round_step

        self._logger.debug(f"Round step is {round_step}")

    def set_round_step(self, round_step: pd.Timedelta) -> None:
        self._logger.debug(f"Round step is set to {round_step}")
        self._round_step = round_step

    def round_value(self, value: pd.Timestamp) -> pd.Timestamp:
        self._logger.debug(f"Rounding value: {value}")
        round_step_in_seconds = int(self._round_step.total_seconds())
        rounded_value = value.floor(f"{round_step_in_seconds}s")
        self._logger.debug(f"Rounded value: {rounded_value}")
        return rounded_value

    def round_series(self, series: pd.Series) -> pd.Series:
        self._logger.debug(f"Rounding series")
        round_step_in_seconds = int(self._round_step.total_seconds())
        rounded_series = series.dt.floor(f"{round_step_in_seconds}s")
        self._logger.debug(f"Series is rounded")
        return rounded_series


class CeilTimestampRoundAlgorithm(AbstractTimestampRoundAlgorithm):

    def __init__(self, round_step: Optional[pd.Timedelta] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._round_step = round_step

        self._logger.debug(f"Round step is {round_step}")

    def set_round_step(self, round_step: pd.Timedelta) -> None:
        self._logger.debug(f"Round step is set to {round_step}")
        self._round_step = round_step

    def round_value(self, value: pd.Timestamp) -> pd.Timestamp:
        self._logger.debug(f"Rounding value: {value}")
        round_step_in_seconds = int(self._round_step.total_seconds())
        rounded_value = value.ceil(f"{round_step_in_seconds}s")
        self._logger.debug(f"Rounded value: {rounded_value}")
        return rounded_value

    def round_series(self, series: pd.Series) -> pd.Series:
        self._logger.debug(f"Rounding series")
        round_step_in_seconds = int(self._round_step.total_seconds())
        rounded_series = series.dt.ceil(f"{round_step_in_seconds}s")
        self._logger.debug(f"Series is rounded")
        return rounded_series


class NearestTimestampRoundAlgorithm(AbstractTimestampRoundAlgorithm):

    def __init__(self, round_step: Optional[pd.Timedelta] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._round_step = round_step

        self._logger.debug(f"Round step is {round_step}")

    def set_round_step(self, round_step: pd.Timedelta) -> None:
        self._logger.debug(f"Round step is set to {round_step}")
        self._round_step = round_step

    def round_value(self, value: pd.Timestamp) -> pd.Timestamp:
        self._logger.debug(f"Rounding value: {value}")
        round_step_in_seconds = int(self._round_step.total_seconds())
        rounded_value = value.round(f"{round_step_in_seconds}s")
        self._logger.debug(f"Rounded value: {rounded_value}")
        return rounded_value

    def round_series(self, series: pd.Series) -> pd.Series:
        self._logger.debug(f"Rounding series")
        round_step_in_seconds = int(self._round_step.total_seconds())
        rounded_series = series.dt.round(f"{round_step_in_seconds}s")
        self._logger.debug(f"Series is rounded")
        return rounded_series