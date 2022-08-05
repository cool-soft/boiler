import random

import pandas as pd
import pytest
from dateutil.tz import gettz

from boiler.constants import column_names, heating_object_types
from boiler.control_action.temp_delta_calculator.single_type_temp_delta_calculator import \
    SingleTypeTempDeltaCalculator

random.seed(10)


class TestSingleTypeTempDeltaCalculator:
    time_step = pd.Timedelta(seconds=60)
    items_count = 10
    start_timestamp = pd.Timestamp.now(tz=gettz("Asia/Yekaterinburg"))
    end_timestamp = start_timestamp + (items_count * time_step)

    @pytest.fixture
    def reaction_df(self):
        reaction_list = []
        current_timestamp = self.start_timestamp
        while current_timestamp <= self.end_timestamp:
            reaction_list.append({
                column_names.TIMESTAMP: current_timestamp,
                column_names.HEATING_OBJ_TYPE: heating_object_types.APARTMENT_HOUSE,
                column_names.HEATING_OBJ_ID: "OBJ_1",
                column_names.FORWARD_TEMP: random.random(),
                column_names.BACKWARD_TEMP: None
            })
            current_timestamp += self.time_step
        reaction_df = pd.DataFrame(reaction_list)
        return reaction_df

    @pytest.fixture
    def requirements_df(self):
        requirements_list = []
        current_timestamp = self.start_timestamp
        while current_timestamp <= self.end_timestamp:
            requirements_list.append({
                column_names.TIMESTAMP: current_timestamp,
                column_names.FORWARD_TEMP: random.random(),
                column_names.BACKWARD_TEMP: random.random()
            })
            current_timestamp += self.time_step
        requirements_df = pd.DataFrame(requirements_list)
        return requirements_df

    @pytest.fixture
    def calculator(self):
        return SingleTypeTempDeltaCalculator()

    def test_calculation(self, reaction_df, requirements_df, calculator):
        temp_delta_df = calculator.calc_temp_delta(reaction_df, requirements_df)
        assert isinstance(temp_delta_df, pd.DataFrame)
        assert not temp_delta_df.empty
        assert len(temp_delta_df) == len(reaction_df)
