import pandas as pd

from boiler.constants import column_names
from boiler.heating_system.model.abstract_heating_system_model import AbstractHeatingSystemModel
from boiler.temp_requirements.constrainst.abstract_temp_requirements_constraint import \
    AbstractTempRequirementsConstraint
from .abstract_control_action_predictor import AbstractControlActionPredictor


class SimpleControlActionPredictor(AbstractControlActionPredictor):

    def __init__(self,
                 heating_system_model: AbstractHeatingSystemModel,
                 temp_requirements_constraint: AbstractTempRequirementsConstraint,
                 min_boiler_temp: float = 30,
                 max_boiler_temp: float = 85,
                 min_regulation_step: float = 0.1
                 ) -> None:
        self._temp_requirements_constraint = temp_requirements_constraint
        self._heating_system_model = heating_system_model

        self._min_boiler_temp = min_boiler_temp
        self._max_boiler_temp = max_boiler_temp
        self._min_regulation_step = min_regulation_step

    def predict_on_weather(self, weather_df: pd.DataFrame) -> pd.DataFrame:
        control_actions_list = []
        for timestamp in weather_df[column_names.TIMESTAMP].to_list():
            a_temp, b_temp = self._min_boiler_temp, self._max_boiler_temp
            while True:
                mean_temp = (a_temp + b_temp) / 2
                if (b_temp - a_temp) <= self._min_regulation_step:
                    control_actions_list.append({
                        column_names.TIMESTAMP: timestamp,
                        column_names.FORWARD_PIPE_COOLANT_TEMP: mean_temp
                    })
                    break
                heating_system_reaction_df = self._heating_system_model.predict_on_boiler_temp(mean_temp)
                if not self._temp_requirements_constraint.check(heating_system_reaction_df, weather_df):
                    a_temp = mean_temp
                else:
                    b_temp = mean_temp
        control_actions_df = pd.DataFrame(control_actions_list)
        return control_actions_df
