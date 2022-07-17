import numpy as np
import pandas as pd
from boiler.logging import logger

from boiler.timedelta.calculators.algo.abstract_time_delta_calculation_algorithm \
    import AbstractTimeDeltaCalculationAlgorithm


class StdVarTimeDeltaCalculationAlgorithm(AbstractTimeDeltaCalculationAlgorithm):

    def __init__(self,
                 x_round_step: float = 0.1,
                 min_lag: int = 1,
                 max_lag: int = 20
                 ) -> None:
        self._x_round_step = x_round_step
        self._min_lag = min_lag
        self._max_lag = max_lag

        logger.debug(
            f"Creating instance:"
            f"x_round_step: {self._x_round_step}"
            f"min_lag: {self._min_lag}"
            f"max_lag: {self._max_lag}"
        )

    def find_lag(self, x: np.ndarray, y: np.ndarray) -> int:
        logger.debug("Lag calculation is requested")

        rounded_x_column = "rounded_x"
        y_column = "y"

        min_std = float("inf")
        lag = None
        for test_lag in range(self._min_lag, self._max_lag):
            x_with_lag = x[:-test_lag]
            y_with_lag = y[test_lag:]

            rounded_x = x_with_lag // self._x_round_step * self._x_round_step

            correlation_df = pd.DataFrame({
                rounded_x_column: rounded_x,
                y_column: y_with_lag
            })
            mean_series = correlation_df.groupby(rounded_x_column)[y_column].mean()
            y_mean_group_value = (correlation_df[rounded_x_column].replace(mean_series)).to_numpy()
            y_delta = y_with_lag - y_mean_group_value

            # noinspection PyUnresolvedReferences
            std_var = np.std(y_delta)

            if std_var < min_std:
                min_std = std_var
                lag = test_lag

        return lag
