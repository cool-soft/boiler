import pytest
import pandas as pd

from boiler.constants import column_names
from boiler.heating_system.model_parameters.corr_table_model_parameters import CorrTableModelParameters


class TestTimedeltaModelRequirementsWOHistory:

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
    def model_parameters(self, timedelta_df):
        return CorrTableModelParameters(timedelta_df)

    def test_min_max_lag_parameters(self, model_parameters, timedelta_df):
        min_timedelta = timedelta_df[column_names.AVG_TIMEDELTA].min()
        assert min_timedelta == model_parameters.get_min_heating_system_lag()

        max_timedelta = timedelta_df[column_names.AVG_TIMEDELTA].max()
        assert max_timedelta == model_parameters.get_max_heating_system_lag()
