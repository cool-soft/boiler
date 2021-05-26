import random

import pytest
import pandas as pd
from dateutil.tz import gettz

from boiler.constants import column_names, heating_object_types
from boiler.data_processing.float_round_algorithm import ArithmeticFloatRoundAlgorithm
from boiler.data_processing.timestamp_round_algorithm import CeilTimestampRoundAlgorithm
from boiler.temp_requirements.predictors.temp_graph_requirements_predictor import TempGraphRequirementsPredictor
from boiler.temp_requirements.constrainst.single_type_heating_obj_constraint import SingleTypeHeatingObjSimpleConstraint

random.seed(10)


class TestSingleTypeHeatingObjConstraint:
    weather_temp_round_decimals = 0
    time_step = pd.Timedelta(seconds=600)
    weather_items_count = 100
    weather_start_timestamp = pd.Timestamp.now(tz=gettz("Asia/Yekaterinburg"))
    weather_end_timestamp = weather_start_timestamp + (weather_items_count * time_step)
    weather_min_temp = -3
    weather_max_temp = 2
    temp_requirements_coefficient = 1.0
    min_model_error = 1.0
    column_true_answer = "true_answer"
    heating_object_type = heating_object_types.APARTMENT_HOUSE
    heating_obj_count = 3

    @pytest.fixture
    def answers(self):
        return [
            True,
            True,
            True,
            False,
            False,
            False
        ]

    @pytest.fixture
    def weather_df(self):
        weather_list = []
        current_timestamp = self.weather_start_timestamp
        while current_timestamp <= self.weather_end_timestamp:
            weather_list.append({
                column_names.TIMESTAMP: current_timestamp,
                column_names.WEATHER_TEMP: random.randint(self.weather_min_temp, self.weather_max_temp)
            })
            current_timestamp += self.time_step
        weather_df = pd.DataFrame(weather_list)
        return weather_df

    @pytest.fixture
    def temp_graph_df(self):
        temp_graph_list = []
        for weather_temp in range(self.weather_min_temp, self.weather_max_temp+1):
            temp_graph_list.append({
                column_names.WEATHER_TEMP: weather_temp,
                column_names.FORWARD_PIPE_COOLANT_TEMP: weather_temp + 50.0,
                column_names.BACKWARD_PIPE_COOLANT_TEMP: weather_temp + 40.0
            })
        return pd.DataFrame(temp_graph_list)

    @pytest.fixture
    def temp_requirements_predictor(self, temp_graph_df):
        return TempGraphRequirementsPredictor(
            temp_graph_df,
            ArithmeticFloatRoundAlgorithm(decimals=self.weather_temp_round_decimals)
        )

    @pytest.fixture
    def reaction_df_list(self, weather_df, answers, temp_requirements_predictor):
        temp_requirements_df = temp_requirements_predictor.predict_on_weather(weather_df)

        reactions_df_list = []
        for i, answer in enumerate(answers):
            reactions_list = []
            if answer:
                for obj_number in range(self.heating_obj_count):
                    requirements_item = temp_requirements_df.sample()
                    reaction = self._generate_true_reaction(obj_number, requirements_item)
                    reactions_list.append(reaction)
            else:
                false_reactions_count = random.randint(1, self.heating_obj_count)
                false_reactions_numbers = random.sample(range(self.heating_obj_count), k=false_reactions_count)
                for obj_number in range(self.heating_obj_count):
                    requirements_item = temp_requirements_df.sample()
                    if obj_number in false_reactions_numbers:
                        reaction = self._generate_false_reaction(obj_number, requirements_item)
                    else:
                        reaction = self._generate_true_reaction(obj_number, requirements_item)
                    reactions_list.append(reaction)
            reactions_df = pd.DataFrame(reactions_list)
            reactions_df_list.append(reactions_df)

        return reactions_df_list

    def _generate_true_reaction(self, obj_number, requirements_item):
        reaction = {
            column_names.TIMESTAMP: requirements_item[column_names.TIMESTAMP].to_list().pop(),
            column_names.HEATING_OBJ_ID: f"obj_{obj_number}",
            column_names.HEATING_OBJ_TYPE: self.heating_object_type,
            column_names.FORWARD_PIPE_COOLANT_TEMP:
                requirements_item[column_names.FORWARD_PIPE_COOLANT_TEMP].to_list().pop()
                + random.randint(3, 5),
            column_names.BACKWARD_PIPE_COOLANT_TEMP: None
        }
        return reaction

    def _generate_false_reaction(self, obj_number, requirements_item):
        reaction = {
            column_names.TIMESTAMP: requirements_item[column_names.TIMESTAMP].to_list().pop(),
            column_names.HEATING_OBJ_ID: f"obj_{obj_number}",
            column_names.HEATING_OBJ_TYPE: self.heating_object_type,
            column_names.FORWARD_PIPE_COOLANT_TEMP:
                requirements_item[column_names.FORWARD_PIPE_COOLANT_TEMP].to_list().pop()
                - random.randint(3, 5),
            column_names.BACKWARD_PIPE_COOLANT_TEMP: None
        }
        return reaction

    @pytest.fixture
    def constraint(self, temp_requirements_predictor):
        return SingleTypeHeatingObjSimpleConstraint(
            temp_requirements_predictor,
            timestamp_round_algo=CeilTimestampRoundAlgorithm(round_step=self.time_step),
            temp_requirements_coefficient=self.temp_requirements_coefficient,
            min_model_error=self.min_model_error,
            heating_obj_type=self.heating_object_type
        )

    def test_base_scenario(self, reaction_df_list, constraint, weather_df, answers):
        for reaction, answer in zip(reaction_df_list, answers):
            assert (constraint.check(reaction, weather_df) >= 0) == answer
