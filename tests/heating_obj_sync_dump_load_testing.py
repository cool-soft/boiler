from random import random

import pandas as pd
import pytest
from dateutil.tz import tzlocal

from boiler.constants import column_names, time_tick, dataset_prototypes


# noinspection PyMethodMayBeStatic
class HeatingObjSyncDumpLoadTesting:

    time_tick_ = time_tick.TIME_TICK
    heating_obj_df_len = 5

    @pytest.fixture
    def heating_obj_df(self):
        heating_obj_df = dataset_prototypes.HEATING_OBJ.copy()

        start_datetime = pd.Timestamp.now(tz=tzlocal())
        end_datetime = start_datetime + (self.heating_obj_df_len * self.time_tick_)

        heating_obj_data_to_append = []
        current_datetime = start_datetime
        while current_datetime <= end_datetime:
            data_part = {}
            for column in heating_obj_df.columns:
                if column != column_names.TIMESTAMP:
                    data_part[column] = round(random(), 2)
            data_part[column_names.TIMESTAMP] = current_datetime
            heating_obj_data_to_append.append(data_part)

            current_datetime += self.time_tick_
        heating_obj_df = heating_obj_df.append(heating_obj_data_to_append)

        return heating_obj_df

    def test_heating_obj_sync_dump_load(self, heating_obj_df, dumper, loader):
        dumper.dump_heating_obj(heating_obj_df)
        loaded_heating_obj_df = loader.load_heating_obj()
        assert loaded_heating_obj_df.to_dict("records") == heating_obj_df.to_dict("records")
