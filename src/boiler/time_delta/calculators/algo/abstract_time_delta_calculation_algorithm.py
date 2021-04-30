import numpy as np


class AbstractTimeDeltaCalculationAlgorithm:

    def find_lag(self, x: np.ndarray, y: np.ndarray) -> int:
        raise NotImplementedError
