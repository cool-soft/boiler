import logging
from typing import List

import numpy as np
import pandas as pd

from boiler.constants import column_names


class CorrTableHeatingSystemModel:

    def __init__(self,
                 temp_correlation_table: pd.DataFrame,
                 home_time_deltas: pd.DataFrame,
                 home_min_temp_coefficient: float = 1.0
                 ) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance of the provider")

        self._temp_correlation_table = temp_correlation_table
        self._homes_time_deltas = home_time_deltas
        self._home_min_temp_coefficient = home_min_temp_coefficient

        self._logger.debug(f"Home min temp coefficient is {home_min_temp_coefficient}")

    # TODO: возвращать np.array
    def predict_on_temp_requirements(self, temp_requirements: np.array) -> List:
        max_home_time_delta = self._homes_time_deltas[column_names.AVG_TIMEDELTA].max()
        boiler_temp_count = len(temp_requirements) - max_home_time_delta

        boiler_temp_list = []
        for time_moment_number in range(boiler_temp_count):
            need_boiler_temp = self._calc_boiler_temp_for_time_moment(time_moment_number, temp_requirements)
            boiler_temp_list.append(need_boiler_temp)

        return boiler_temp_list

    def _calc_boiler_temp_for_time_moment(self,
                                          time_moment_number: int,
                                          temp_requirements_arr: np.array) -> float:
        need_boiler_temp = float("-inf")

        home_names_list = self._homes_time_deltas[column_names.HEATING_OBJ_ID].to_list()
        time_deltas_list = self._homes_time_deltas[column_names.AVG_TIMEDELTA].to_list()
        for home_name, home_time_delta in zip(home_names_list, time_deltas_list):
            need_home_temp = temp_requirements_arr[time_moment_number + home_time_delta]
            need_home_temp *= self._home_min_temp_coefficient
            need_temp_condition = self._temp_correlation_table[home_name] >= need_home_temp
            need_boiler_temp_for_home = self._temp_correlation_table[
                need_temp_condition][column_names.CORRELATED_BOILER_TEMP].min()
            need_boiler_temp = max(need_boiler_temp, need_boiler_temp_for_home)

        return need_boiler_temp
