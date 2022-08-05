from typing import Tuple

import pandas as pd
from boiler.logging import logger

from boiler.constants import column_names
from boiler.data_processing.float_round_algorithm import AbstractFloatRoundAlgorithm
from .abstract_temp_requirements_calculator import AbstractTempRequirementsCalculator


class TempGraphRequirementsCalculator(AbstractTempRequirementsCalculator):

    def __init__(self,
                 temp_graph: pd.DataFrame,
                 weather_temp_round_algorithm: AbstractFloatRoundAlgorithm
                 ) -> None:
        self._temp_graph = temp_graph
        self._weather_temp_round_algo = weather_temp_round_algorithm

        logger.debug("Creating instance")

    def calc_for_weather(self, weather_df: pd.DataFrame) -> pd.DataFrame:
        logger.debug(f"Predicting on weather is requested; weather_df len = {len(weather_df)}")
        weather_df = self._round_weather_temp(weather_df)
        temp_requirements_df = self._calc_for_weather_df(weather_df)
        return temp_requirements_df

    def _round_weather_temp(self, weather_df: pd.DataFrame) -> pd.DataFrame:
        weather_df = weather_df.copy()
        weather_df[column_names.WEATHER_TEMP] = \
            self._weather_temp_round_algo.round_series(weather_df[column_names.WEATHER_TEMP])
        return weather_df

    def _calc_for_weather_df(self, weather_df: pd.DataFrame) -> pd.DataFrame:
        temp_requirements_list = []
        weather_temp_arr = weather_df[column_names.WEATHER_TEMP].to_numpy()
        timestamp_list = weather_df[column_names.TIMESTAMP].to_list()
        for weather_temp, timestamp in zip(weather_temp_arr, timestamp_list):
            forward_temp, backward_temp = self._calc_for_weather_temp(weather_temp)
            temp_requirements_list.append({
                column_names.TIMESTAMP: timestamp,
                column_names.FORWARD_TEMP: forward_temp,
                column_names.BACKWARD_TEMP: backward_temp
            })
        temp_requirements_df = pd.DataFrame(temp_requirements_list)
        return temp_requirements_df

    def _calc_for_weather_temp(self, weather_temp: float) -> Tuple[float, float]:
        available_temp = self._temp_graph[self._temp_graph[column_names.WEATHER_TEMP] <= weather_temp]
        if not available_temp.empty:
            required_temp_idx = available_temp[column_names.WEATHER_TEMP].idxmax()
        else:
            required_temp_idx = self._temp_graph[column_names.WEATHER_TEMP].idxmin()
            logger.warning(f"Weather temp {weather_temp} is not in temp graph")

        forward_temp = self._temp_graph.loc[required_temp_idx, column_names.FORWARD_TEMP]
        backward_temp = self._temp_graph.loc[required_temp_idx, column_names.BACKWARD_TEMP]
        return forward_temp, backward_temp
