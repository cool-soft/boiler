import logging

import pandas as pd


class AbstractTimestampRoundAlgorithm:

    def round_value(self, value: pd.Timestamp) -> pd.Timestamp:
        raise NotImplementedError

    def round_series(self, series: pd.Series) -> pd.Series:
        raise NotImplementedError


class FloorTimestampRoundAlgorithm(AbstractTimestampRoundAlgorithm):

    def __init__(self, round_step: pd.Timedelta) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)

        self._round_step = round_step

    def round_value(self, value: pd.Timestamp) -> pd.Timestamp:
        self._logger.debug(f"Rounding value: {value}")
        round_step_in_seconds = int(self._round_step.total_seconds())
        rounded_value = value.floor(f"{round_step_in_seconds}s")
        self._logger.debug(f"Rounded value: {rounded_value}")
        return rounded_value

    def round_series(self, series: pd.Series) -> pd.Series:
        self._logger.debug("Rounding series")
        round_step_in_seconds = int(self._round_step.total_seconds())
        rounded_series = series.dt.floor(f"{round_step_in_seconds}s")
        self._logger.debug("Series is rounded")
        return rounded_series


class CeilTimestampRoundAlgorithm(AbstractTimestampRoundAlgorithm):

    def __init__(self, round_step: pd.Timedelta) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)

        self._round_step = round_step

    def round_value(self, value: pd.Timestamp) -> pd.Timestamp:
        # self._logger.debug(f"Rounding value: {value}")
        round_step_in_seconds = int(self._round_step.total_seconds())
        rounded_value = value.ceil(f"{round_step_in_seconds}s")
        # self._logger.debug(f"Rounded value: {rounded_value}")
        return rounded_value

    def round_series(self, series: pd.Series) -> pd.Series:
        # self._logger.debug("Rounding series")
        round_step_in_seconds = int(self._round_step.total_seconds())
        rounded_series = series.dt.ceil(f"{round_step_in_seconds}s")
        # self._logger.debug("Series is rounded")
        return rounded_series


class NearestTimestampRoundAlgorithm(AbstractTimestampRoundAlgorithm):

    def __init__(self, round_step: pd.Timedelta) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)

        self._round_step = round_step

    def round_value(self, value: pd.Timestamp) -> pd.Timestamp:
        self._logger.debug(f"Rounding value: {value}")
        round_step_in_seconds = int(self._round_step.total_seconds())
        rounded_value = value.round(f"{round_step_in_seconds}s")
        self._logger.debug(f"Rounded value: {rounded_value}")
        return rounded_value

    def round_series(self, series: pd.Series) -> pd.Series:
        self._logger.debug("Rounding series")
        round_step_in_seconds = int(self._round_step.total_seconds())
        rounded_series = series.dt.round(f"{round_step_in_seconds}s")
        self._logger.debug("Series is rounded")
        return rounded_series
