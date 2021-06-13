import pytest
import pandas as pd

from boiler.constants import column_names
from boiler.heating_system.model_requirements.timedelta_model_requirements_without_history import \
    TimedeltaModelRequirementsWithoutHistory


class TestTimedeltaModelRequirementsWOHistory:

    control_action_timestamp = pd.Timestamp("2017-01-01T12:00+05:00")

    @pytest.fixture
    def timedelta_df(self):
        return pd.DataFrame([
            {
                column_names.HEATING_OBJ_ID: "building_1",
                column_names.AVG_TIMEDELTA: pd.Timedelta(seconds=100)
            },
            {
                column_names.HEATING_OBJ_ID: "building_2",
                column_names.AVG_TIMEDELTA: pd.Timedelta(seconds=40)
            },
            {
                column_names.HEATING_OBJ_ID: "building_3",
                column_names.AVG_TIMEDELTA: pd.Timedelta(seconds=500)
            },
            {
                column_names.HEATING_OBJ_ID: "building_4",
                column_names.AVG_TIMEDELTA: pd.Timedelta(seconds=600)
            },

        ])

    @pytest.fixture
    def model_requirements_calculator(self, timedelta_df):
        return TimedeltaModelRequirementsWithoutHistory(timedelta_df)

    def test_weather_timestamp_requirements(self, model_requirements_calculator, timedelta_df):
        min_timedelta = timedelta_df[column_names.AVG_TIMEDELTA].min()
        max_timedelta = timedelta_df[column_names.AVG_TIMEDELTA].max()
        start_weather_timestamp = self.control_action_timestamp + min_timedelta
        end_weather_timestamp = self.control_action_timestamp + max_timedelta

        weather_borders = model_requirements_calculator.get_weather_start_end_timestamps(self.control_action_timestamp)
        assert (start_weather_timestamp, end_weather_timestamp) == weather_borders

    def test_system_states_history_requirements(self, model_requirements_calculator):
        borders_df = model_requirements_calculator.get_heating_states_history_timestamps(self.control_action_timestamp)
        assert borders_df.empty
