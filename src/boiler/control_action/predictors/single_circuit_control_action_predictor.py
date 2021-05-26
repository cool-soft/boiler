import pandas as pd

from boiler.constants import column_names, circuit_types
from boiler.heating_system.model.abstract_heating_system_model import AbstractHeatingSystemModel
from boiler.temp_requirements.constrainst.abstract_temp_requirements_constraint import \
    AbstractTempRequirementsConstraint
from .abstract_control_action_predictor import AbstractControlActionPredictor


class SingleCircuitControlActionPredictor(AbstractControlActionPredictor):

    def __init__(self,
                 heating_system_model: AbstractHeatingSystemModel,
                 temp_requirements_constraint: AbstractTempRequirementsConstraint,
                 controlled_circuit_type: str = circuit_types.HEATING,
                 min_boiler_temp: float = 30,
                 max_boiler_temp: float = 85,
                 min_regulation_step: float = 0.3,
                 ) -> None:
        self._heating_system_model = heating_system_model
        self._temp_requirements_constraint = temp_requirements_constraint

        self._min_boiler_temp = min_boiler_temp
        self._max_boiler_temp = max_boiler_temp
        self._min_regulation_step = min_regulation_step
        self._controlled_circuit_type = controlled_circuit_type

    def predict_one(self,
                    weather_forecast_df: pd.DataFrame,
                    system_states_history_df: pd.DataFrame,
                    control_action_timestamp: pd.Timestamp
                    ) -> pd.DataFrame:
        a_temp, b_temp = self._min_boiler_temp, self._max_boiler_temp
        while True:
            mean_temp = (a_temp + b_temp) / 2
            control_action_df = self._create_control_action_df(control_action_timestamp, mean_temp)
            if (b_temp - a_temp) <= 2 * self._min_regulation_step:
                break
            heating_system_reaction_df = self._heating_system_model.predict(
                weather_forecast_df,
                system_states_history_df,
                control_action_df
            )
            temp_delta = self._temp_requirements_constraint.check(
                heating_system_reaction_df,
                weather_forecast_df
            )
            if temp_delta < 0:
                a_temp = mean_temp
            else:
                b_temp = mean_temp

        return control_action_df

    def _create_control_action_df(self,
                                  control_action_timestamp: pd.Timestamp,
                                  action_temp: float
                                  ) -> pd.DataFrame:
        control_action_df = pd.DataFrame([{
            column_names.TIMESTAMP: control_action_timestamp,
            column_names.FORWARD_PIPE_COOLANT_TEMP: action_temp,
            column_names.CIRCUIT_TYPE: self._controlled_circuit_type
        }])
        return control_action_df
