import logging
from typing import Dict

import pandas as pd
from .abstract_temp_requirements_predictor import AbstractTempRequirementsPredictor
from boiler.constants import column_names


class TempGraphRequirementsPredictor(AbstractTempRequirementsPredictor):

    def __init__(self,
                 temp_graph: pd.DataFrame
                 ) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._temp_graph = temp_graph

    def predict_on_weather(self, weather_df: pd.DataFrame) -> pd.DataFrame:
        temp_requirements_list = []
        weather_temp_arr = weather_df[column_names.WEATHER_TEMP].to_numpy()
        timestamp_list = weather_df[column_names.TIMESTAMP].to_list()
        for weather_temp, timestamp in zip(weather_temp_arr, timestamp_list):
            temp_requirements = self._calc_for_weather_temp(weather_temp)

            required_forward_temp = temp_requirements[column_names.FORWARD_PIPE_COOLANT_TEMP]
            required_backward_temp = temp_requirements[column_names.BACKWARD_PIPE_COOLANT_TEMP]
            temp_requirements_list.append({
                column_names.TIMESTAMP: timestamp,
                column_names.FORWARD_PIPE_COOLANT_TEMP: required_forward_temp,
                column_names.BACKWARD_PIPE_COOLANT_TEMP: required_backward_temp
            })

        temp_requirements_df = pd.DataFrame(temp_requirements_list)
        return temp_requirements_df

    def _calc_for_weather_temp(self, weather_temp: float) -> Dict[str, float]:
        available_temp = self._temp_graph[self._temp_graph[column_names.WEATHER_TEMP] <= weather_temp]
        if not available_temp.empty:
            required_temp_idx = available_temp[column_names.WEATHER_TEMP].idxmax()
        else:
            required_temp_idx = self._temp_graph[column_names.WEATHER_TEMP].idxmin()
            self._logger.info(f"Weather temp {weather_temp} is not in temp graph")

        forward_temp = self._temp_graph.loc[required_temp_idx, column_names.FORWARD_PIPE_COOLANT_TEMP]
        backward_temp = self._temp_graph.loc[required_temp_idx, column_names.BACKWARD_PIPE_COOLANT_TEMP]

        required_temp = {
            column_names.FORWARD_PIPE_COOLANT_TEMP: forward_temp,
            column_names.BACKWARD_PIPE_COOLANT_TEMP: backward_temp
        }

        return required_temp
