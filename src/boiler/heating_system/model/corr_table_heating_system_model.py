import logging

import pandas as pd

from boiler.constants import column_names
from .abstract_heating_system_model import AbstractHeatingSystemModel


class CorrTableHeatingSystemModel(AbstractHeatingSystemModel):

    def __init__(self,
                 temp_correlation_df: pd.DataFrame,
                 objects_type: str,
                 timedelta_df: pd.DataFrame,
                 ) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._temp_correlation_df = temp_correlation_df
        self._timedelta_df = timedelta_df
        self._objects_type = objects_type

    def predict_on_boiler_temp(self, boiler_temp: float) -> pd.DataFrame:
        heating_system_reaction = []
        heating_obj_ids = self._timedelta_df[column_names.HEATING_OBJ_ID].to_list()
        timedelta_list = self._timedelta_df[column_names.AVG_TIMEDELTA].to_list()
        for obj_id, timedelta in zip(heating_obj_ids, timedelta_list):
            forward_pipe_temp = self._calc_forward_pipe_coolant_temp_for_object(obj_id, boiler_temp)
            reaction = {
                column_names.HEATING_OBJ_ID: obj_id,
                column_names.HEATING_OBJ_TYPE: self._objects_type,
                column_names.TIMEDELTA: timedelta,
                column_names.FORWARD_PIPE_COOLANT_TEMP: forward_pipe_temp,
                column_names.BACKWARD_PIPE_COOLANT_TEMP: None
            }
            heating_system_reaction.append(reaction)

        heating_system_reaction_df = pd.DataFrame(heating_system_reaction)
        return heating_system_reaction_df

    def _calc_forward_pipe_coolant_temp_for_object(self, obj_id: str, boiler_temp: float) -> float:
        temps = self._temp_correlation_df[
            self._temp_correlation_df[column_names.CORRELATED_BOILER_TEMP] <= boiler_temp
            ]
        forward_pipe_temp = temps[obj_id].max()
        return forward_pipe_temp
