import pandas as pd
from boiler.logger import boiler_logger


class AbstractTimestampRoundAlgorithm:

    def round_value(self, value: pd.Timestamp) -> pd.Timestamp:
        raise NotImplementedError

    def round_series(self, series: pd.Series) -> pd.Series:
        raise NotImplementedError


class FloorTimestampRoundAlgorithm(AbstractTimestampRoundAlgorithm):

    def __init__(self, round_step: pd.Timedelta) -> None:
        self._round_step = round_step
        boiler_logger.debug(
            f"Creating instance:"
            f"round step: {round_step}"
        )

    def round_value(self, value: pd.Timestamp) -> pd.Timestamp:
        boiler_logger.debug(f"Rounding value: {value}")
        round_step_in_seconds = int(self._round_step.total_seconds())
        rounded_value = value.floor(f"{round_step_in_seconds}s")
        return rounded_value

    def round_series(self, series: pd.Series) -> pd.Series:
        boiler_logger.debug(f"Rounding series; series len = {len(series)}")
        round_step_in_seconds = int(self._round_step.total_seconds())
        rounded_series = series.dt.floor(f"{round_step_in_seconds}s")
        return rounded_series


class CeilTimestampRoundAlgorithm(AbstractTimestampRoundAlgorithm):

    def __init__(self, round_step: pd.Timedelta) -> None:
        self._round_step = round_step
        boiler_logger.debug(
            f"Creating instance:"
            f"round step: {round_step}"
        )

    def round_value(self, value: pd.Timestamp) -> pd.Timestamp:
        boiler_logger.debug(f"Rounding value: {value}")
        round_step_in_seconds = int(self._round_step.total_seconds())
        rounded_value = value.ceil(f"{round_step_in_seconds}s")
        return rounded_value

    def round_series(self, series: pd.Series) -> pd.Series:
        boiler_logger.debug(f"Rounding series; series len = {len(series)}")
        round_step_in_seconds = int(self._round_step.total_seconds())
        rounded_series = series.dt.ceil(f"{round_step_in_seconds}s")
        return rounded_series


class NearestTimestampRoundAlgorithm(AbstractTimestampRoundAlgorithm):

    def __init__(self, round_step: pd.Timedelta) -> None:
        self._round_step = round_step
        boiler_logger.debug(
            f"Creating instance:"
            f"round step: {round_step}"
        )

    def round_value(self, value: pd.Timestamp) -> pd.Timestamp:
        boiler_logger.debug(f"Rounding value: {value}")
        round_step_in_seconds = int(self._round_step.total_seconds())
        rounded_value = value.round(f"{round_step_in_seconds}s")
        return rounded_value

    def round_series(self, series: pd.Series) -> pd.Series:
        boiler_logger.debug(f"Rounding series; series len = {len(series)}")
        round_step_in_seconds = int(self._round_step.total_seconds())
        rounded_series = series.dt.round(f"{round_step_in_seconds}s")
        return rounded_series
