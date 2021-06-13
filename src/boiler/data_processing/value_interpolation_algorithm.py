import pandas as pd
from boiler.logger import boiler_logger


class AbstractValueInterpolationAlgorithm:

    def interpolate_series(self, series: pd.Series) -> pd.Series:
        raise NotImplementedError


class LinearInsideValueInterpolationAlgorithm(AbstractValueInterpolationAlgorithm):

    def __init__(self) -> None:
        boiler_logger.debug("Creating instance")

    def interpolate_series(self, series: pd.Series) -> pd.Series:
        boiler_logger.debug(f"Interpolating series; series len = {len(series)}")
        series = pd.to_numeric(series, downcast="float")
        interpolated_series = series.interpolate(method="linear")
        return interpolated_series


class LinearOutsideValueInterpolationAlgorithm(AbstractValueInterpolationAlgorithm):

    def __init__(self) -> None:
        boiler_logger.debug("Creating instance")

    def interpolate_series(self, series: pd.Series) -> pd.Series:
        boiler_logger.debug(f"Interpolating series; series len = {len(series)}")
        series = pd.to_numeric(series, downcast="float")
        interpolated_series = series.interpolate(method="linear", limit_area="outside", limit_direction="both")
        return interpolated_series
