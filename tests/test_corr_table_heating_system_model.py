import random

import pandas as pd
import pytest
from dateutil.tz import gettz

from boiler.constants import column_names, heating_object_types, circuit_types, dataset_prototypes
from boiler.heating_system.model.corr_table_heating_system_model import CorrTableHeatingSystemModel

random.seed(10)


class TestCorrTableHeatingSystemModel:
    timedelta = pd.Timedelta(seconds=60)

    control_timestamp = pd.Timestamp.now(tz=gettz("Asia/Yekaterinburg"))
    circuit_type = circuit_types.HEATING
    heating_object_type = heating_object_types.APARTMENT_HOUSE

    weather_start_timestamp = control_timestamp
    weather_end_timestamp = weather_start_timestamp + (100 * timedelta)

    obj_and_timedelta = {
        "obj_1": pd.Timedelta(seconds=900),
        "obj_2": pd.Timedelta(seconds=500)
    }

    boiler_corr_table_column = column_names.CORRELATED_BOILER_TEMP
    corr_table_rows_count = 5
    min_corr_temp = 20
    max_corr_temp = 60

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
    def weather_df(self):
        weather_list = []
        current_timestamp = self.weather_start_timestamp
        while current_timestamp <= self.weather_end_timestamp:
            weather_list.append({
                column_names.TIMESTAMP: current_timestamp,
                column_names.WEATHER_TEMP: random.random()
            })
            current_timestamp += self.timedelta
        weather_df = pd.DataFrame(weather_list)
        return weather_df

    @pytest.fixture
    def heating_system_state_history_df(self):
        return dataset_prototypes.HEATING_SYSTEM_STATE.copy()

    def test_heating_system_model(self,
                                  heating_system_model,
                                  weather_df,
                                  timedelta_df,
                                  heating_system_state_history_df):
        current_temp = self.min_corr_temp
        temp_step = (self.max_corr_temp - self.min_corr_temp) / self.corr_table_rows_count
        while current_temp <= self.max_corr_temp:
            control_action_df = pd.DataFrame([{
                column_names.TIMESTAMP: self.control_timestamp,
                column_names.CIRCUIT_TYPE: self.circuit_type,
                column_names.FORWARD_PIPE_COOLANT_TEMP: current_temp
            }])
            system_reaction_df = heating_system_model.predict(
                weather_df,
                heating_system_state_history_df,
                control_action_df
            )

            for column_name in dataset_prototypes.HEATING_SYSTEM_STATE.columns.to_list():
                assert column_name in system_reaction_df

            assert system_reaction_df[column_names.FORWARD_PIPE_COOLANT_TEMP].notnull().all() or\
                system_reaction_df[column_names.BACKWARD_PIPE_COOLANT_TEMP].notnull().all()

            for obj_id, timedelta in self.obj_and_timedelta.items():
                assert obj_id in system_reaction_df[column_names.HEATING_OBJ_ID].to_list()
                need_reaction_timestamp = self.control_timestamp + timedelta
                obj_reaction = system_reaction_df[
                    system_reaction_df[column_names.HEATING_OBJ_ID] == obj_id
                ]
                assert obj_reaction[column_names.TIMESTAMP].to_list().pop() == need_reaction_timestamp

            current_temp += temp_step
