import logging

import pandas as pd


class AbstractValueInterpolationAlgorithm:

    def interpolate_series(self, series: pd.Series) -> pd.Series:
        raise NotImplementedError


class LinearInsideValueInterpolationAlgorithm(AbstractValueInterpolationAlgorithm):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

    def interpolate_series(self, series: pd.Series) -> pd.Series:
        self._logger.debug("Interpolating is requested")
        series = pd.to_numeric(series, downcast="float")
        interpolated_series = series.interpolate(method="linear")
        self._logger.debug("Interpolated")
        return interpolated_series


class LinearOutsideValueInterpolationAlgorithm(AbstractValueInterpolationAlgorithm):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

    def interpolate_series(self, series: pd.Series) -> pd.Series:
        self._logger.debug("Interpolating is requested")
        series = pd.to_numeric(series, downcast="float")
        interpolated_series = series.interpolate(method="linear", limit_area="outside", limit_direction="both")
        self._logger.debug("Interpolated")
        return interpolated_series
