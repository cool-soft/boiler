import math
import random
import pandas as pd
import pytest
from boiler.control_action.predictors.single_circuit_control_action_predictor import SingleCircuitControlActionPredictor

from boiler.data_processing.timestamp_round_algorithm import CeilTimestampRoundAlgorithm
from boiler.temp_requirements.constraint.single_type_heating_obj_on_weather_constraint \
    import SingleTypeHeatingObjOnWeatherConstraint
from dateutil.tz import gettz

from boiler.constants import circuit_types, heating_object_types, column_names, dataset_prototypes
from boiler.data_processing.float_round_algorithm import ArithmeticFloatRoundAlgorithm
from boiler.heating_system.model.corr_table_heating_system_model import CorrTableHeatingSystemModel
from boiler.temp_requirements.predictors.temp_graph_requirements_predictor import TempGraphRequirementsPredictor

random.seed(10)


class TestSingleCircuitControlActionPredictor:
    timedelta = pd.Timedelta(seconds=60)

    control_timestamp = pd.Timestamp.now(tz=gettz("Asia/Yekaterinburg"))
    circuit_type = circuit_types.HEATING
    heating_object_type = heating_object_types.APARTMENT_HOUSE

    obj_and_timedelta = {
        "obj_1": pd.Timedelta(seconds=120),
        "obj_2": pd.Timedelta(seconds=180)
    }

    max_obj_timedelta = max(obj_and_timedelta.values())

    weather_start_timestamp = control_timestamp
    weather_end_timestamp = weather_start_timestamp + (math.ceil(max_obj_timedelta / timedelta) * timedelta)

    corr_table_rows_count = 10
    min_corr_temp = 20
    max_corr_temp = 90

    boiler_corr_table_column = column_names.CORRELATED_BOILER_TEMP

    min_temp_graph_weather_temp = -30
    max_temp_graph_weather_temp = 30
    min_temp_graph_required_temp = min_corr_temp
    max_temp_graph_required_temp = max_corr_temp
    temp_graph_rows_count = 10

    min_boiler_temp = min_corr_temp + 10
    max_boiler_temp = max_corr_temp - 10
    regulation_step = 0.3

    @pytest.fixture
    def corr_table_df(self):
        corr_table = []
        current_temp = self.min_corr_temp
        temp_step = (self.max_corr_temp - self.min_corr_temp) / self.corr_table_rows_count
        while current_temp <= self.max_corr_temp:
            correlated_temp = {
                self.boiler_corr_table_column: current_temp
            }
            for i, obj_id in enumerate(self.obj_and_timedelta.keys(), start=1):
                correlated_temp[obj_id] = current_temp - i
            current_temp += temp_step
            corr_table.append(correlated_temp)
        return pd.DataFrame(corr_table)

    @pytest.fixture
    def timedelta_df(self):
        timedelta_list = []
        for obj_id, timedelta in self.obj_and_timedelta.items():
            timedelta_list.append({
                column_names.HEATING_OBJ_ID: obj_id,
                column_names.AVG_TIMEDELTA: timedelta
            })
        return pd.DataFrame(timedelta_list)

    @pytest.fixture
    def heating_system_model(self, timedelta_df, corr_table_df):
        return CorrTableHeatingSystemModel(
            temp_correlation_df=corr_table_df,
            timedelta_df=timedelta_df,
            circuit_type=circuit_types.HEATING,
            objects_type=heating_object_types.APARTMENT_HOUSE
        )

    @pytest.fixture
    def temp_graph_df(self):
        temp_graph_list = []
        current_weather_temp = self.min_temp_graph_weather_temp
        weather_temp_step = (self.max_temp_graph_weather_temp -
                             self.min_temp_graph_weather_temp) / (self.temp_graph_rows_count-1)
        current_required_temp = self.max_temp_graph_required_temp
        required_temp_step = (self.max_temp_graph_required_temp -
                              self.min_temp_graph_required_temp) / (self.temp_graph_rows_count-1)

        for i in range(self.temp_graph_rows_count):
            requirements = {
                column_names.WEATHER_TEMP: current_weather_temp,
                column_names.FORWARD_PIPE_COOLANT_TEMP: current_required_temp,
                column_names.BACKWARD_PIPE_COOLANT_TEMP: current_required_temp - 1
            }
            temp_graph_list.append(requirements)
            current_weather_temp += weather_temp_step
            current_required_temp -= required_temp_step

        return pd.DataFrame(temp_graph_list)

    @pytest.fixture
    def temp_requirements_predictor(self, temp_graph_df):
        return TempGraphRequirementsPredictor(
            temp_graph=temp_graph_df,
            weather_temp_round_algorithm=ArithmeticFloatRoundAlgorithm()
        )

    @pytest.fixture
    def constraint(self, temp_requirements_predictor):
        return SingleTypeHeatingObjOnWeatherConstraint(
            temp_requirements_predictor,
            timestamp_round_algo=CeilTimestampRoundAlgorithm(round_step=self.timedelta),
            temp_requirements_coefficient=1.0,
            min_model_error=0.0,
            heating_obj_type=self.heating_object_type
        )

    @pytest.fixture
    def control_action_predictor(self, heating_system_model, constraint):
        return SingleCircuitControlActionPredictor(
            heating_system_model=heating_system_model,
            temp_requirements_constraint=constraint,
            controlled_circuit_type=self.circuit_type,
            min_boiler_temp=self.min_boiler_temp,
            max_boiler_temp=self.max_boiler_temp,
            min_regulation_step=self.regulation_step,
        )

    @pytest.fixture
    def heating_system_state_history_df(self):
        return dataset_prototypes.HEATING_SYSTEM_STATE.copy()

    def generate_weather_df(self, weather_temp):
        weather_list = []
        current_timestamp = self.weather_start_timestamp
        while current_timestamp <= self.weather_end_timestamp:
            weather_list.append({
                column_names.TIMESTAMP: current_timestamp,
                column_names.WEATHER_TEMP: weather_temp,
            })
            current_timestamp += self.timedelta
        return pd.DataFrame(weather_list)

    def test_on_over_min_weather_temp(self,
                                      control_action_predictor,
                                      temp_graph_df,
                                      heating_system_state_history_df):
        weather_df = self.generate_weather_df(temp_graph_df[column_names.WEATHER_TEMP].min()-10)
        control_action = control_action_predictor.predict_one(
            weather_df,
            self.control_timestamp
        )
        assert len(control_action) == 1
        assert abs(control_action[column_names.FORWARD_PIPE_COOLANT_TEMP].to_list().pop()
                   - self.max_boiler_temp) <= self.regulation_step

    def test_on_over_max_weather_temp(self,
                                      control_action_predictor,
                                      temp_graph_df,
                                      heating_system_state_history_df):
        weather_df = self.generate_weather_df(temp_graph_df[column_names.WEATHER_TEMP].max()+10)
        control_action = control_action_predictor.predict_one(
            weather_df,
            self.control_timestamp
        )
        assert len(control_action) == 1
        assert abs(control_action[column_names.FORWARD_PIPE_COOLANT_TEMP].to_list().pop()
                   - self.min_boiler_temp) <= self.regulation_step
