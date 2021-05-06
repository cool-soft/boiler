import logging

import numpy as np

from boiler.timedelta.calculators.algo.abstract_time_delta_calculation_algorithm \
    import AbstractTimeDeltaCalculationAlgorithm


class CorrTimeDeltaCalculationAlgorithm(AbstractTimeDeltaCalculationAlgorithm):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

    def find_lag(self, x: np.ndarray, y: np.ndarray) -> int:
        self._logger.debug("Lag calculation is requested")

        x -= x.mean()
        x /= x.std()

        y -= y.mean()
        y /= y.std()

        corr = np.correlate(y, x)
        lag = corr.argmax() + 1 - len(y)

        return lag
