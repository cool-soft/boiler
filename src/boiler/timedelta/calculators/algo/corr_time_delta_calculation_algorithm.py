import numpy as np
from boiler.logging import logger

from boiler.timedelta.calculators.algo.abstract_time_delta_calculation_algorithm \
    import AbstractTimeDeltaCalculationAlgorithm


class CorrTimeDeltaCalculationAlgorithm(AbstractTimeDeltaCalculationAlgorithm):

    def __init__(self) -> None:
        logger.debug("Creating instance")

    def find_lag(self, x: np.ndarray, y: np.ndarray) -> int:
        logger.debug("Lag calculation is requested")

        x -= x.mean()
        x /= x.std()

        y -= y.mean()
        y /= y.std()

        corr = np.correlate(y, x)
        lag = corr.argmax() + 1 - len(y)

        return lag
