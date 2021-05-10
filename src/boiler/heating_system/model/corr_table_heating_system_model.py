import logging

import pandas as pd

from boiler.constants import column_names, circuit_types, heating_object_types
from .abstract_heating_system_model import AbstractHeatingSystemModel


class CorrTableHeatingSystemModel(AbstractHeatingSystemModel):

    def __init__(self,
                 temp_correlation_df: pd.DataFrame,
                 timedelta_df: pd.DataFrame,
                 objects_type: str = heating_object_types.APARTMENT_HOUSE,
                 circuit_type: str = circuit_types.HEATING
                 ) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._temp_correlation_df = temp_correlation_df
        self._timedelta_df = timedelta_df
        self._objects_type = objects_type
        self._circuit_type = circuit_type

    def predict(self,
                weather_df: pd.DataFrame,
                system_state_history_df: pd.DataFrame,
                control_action_df: pd.DataFrame
                ) -> pd.DataFrame:
        boiler_temp, control_action_timestamp = self._unpack_control_action(control_action_df)

        heating_system_reaction = []
        heating_obj_ids = self._timedelta_df[column_names.HEATING_OBJ_ID].to_list()
        timedelta_list = self._timedelta_df[column_names.AVG_TIMEDELTA].to_list()
        for obj_id, timedelta in zip(heating_obj_ids, timedelta_list):
            coolant_temp = self._calc_coolant_temp_for_object(obj_id, boiler_temp)
            heating_system_reaction.append({
                column_names.TIMESTAMP: control_action_timestamp + timedelta,
                column_names.HEATING_OBJ_ID: obj_id,
                column_names.HEATING_OBJ_TYPE: self._objects_type,
                column_names.CIRCUIT_TYPE: self._circuit_type,
                column_names.FORWARD_PIPE_COOLANT_TEMP: coolant_temp,
                column_names.BACKWARD_PIPE_COOLANT_TEMP: None
            })

        return pd.DataFrame(heating_system_reaction)

    def _unpack_control_action(self, control_action_df: pd.DataFrame):
        control_action_df = control_action_df[
            control_action_df[column_names.CIRCUIT_TYPE] == self._circuit_type
        ].copy()
        last_timestamp_idx = control_action_df[column_names.TIMESTAMP].idxmax()
        control_action_timestamp = control_action_df.loc[last_timestamp_idx, column_names.TIMESTAMP]
        boiler_temp = control_action_df.loc[last_timestamp_idx, column_names.FORWARD_PIPE_COOLANT_TEMP]
        return boiler_temp, control_action_timestamp

    def _calc_coolant_temp_for_object(self, obj_id: str, boiler_temp: float) -> float:
        temps = self._temp_correlation_df[
            self._temp_correlation_df[column_names.CORRELATED_BOILER_TEMP] <= boiler_temp
            ]
        forward_pipe_temp = temps[obj_id].max()
        return forward_pipe_temp
