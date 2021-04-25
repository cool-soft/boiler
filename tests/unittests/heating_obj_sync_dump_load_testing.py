from random import random

import pandas as pd
import pytest
from dateutil.tz import tzlocal

from boiler.constants import column_names, time_tick


# noinspection PyMethodMayBeStatic
class HeatingObjSyncDumpLoadTesting:

    @pytest.fixture
    def time_tick_(self):
        return time_tick.TIME_TICK

    @pytest.fixture
    def heating_obj_df(self, time_tick_):
        columns = (
            column_names.FORWARD_PIPE_COOLANT_TEMP,
            column_names.BACKWARD_PIPE_COOLANT_TEMP,
            column_names.FORWARD_PIPE_COOLANT_VOLUME,
            column_names.BACKWARD_PIPE_COOLANT_VOLUME,
            column_names.FORWARD_PIPE_COOLANT_PRESSURE,
            column_names.BACKWARD_PIPE_COOLANT_PRESSURE
        )

        heating_obj_df = pd.DataFrame(
            columns=(column_names.TIMESTAMP, *columns)
        )

        heating_obj_df_len = 5
        start_datetime = pd.Timestamp.now(tz=tzlocal())
        end_datetime = start_datetime + (heating_obj_df_len * time_tick_)

        heating_obj_data_to_append = []
        current_datetime = start_datetime
        while current_datetime <= end_datetime:
            data_part = {}
            for column in columns:
                data_part[column] = round(random(), 2)
            data_part[column_names.TIMESTAMP] = current_datetime
            heating_obj_data_to_append.append(data_part)

            current_datetime += time_tick_
        heating_obj_df = heating_obj_df.append(heating_obj_data_to_append)

        return heating_obj_df

    def test_heating_obj_sync_dump_load(self, heating_obj_df, dumper, loader):
        dumper.dump_heating_obj(heating_obj_df)
        loaded_heating_obj_df = loader.load_heating_obj()
        assert loaded_heating_obj_df.to_dict("records") == heating_obj_df.to_dict("records")
