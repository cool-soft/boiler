from random import random

import pandas as pd
import pytest
from dateutil.tz import tzlocal

from boiler.constants import column_names, time_tick


# noinspection PyMethodMayBeStatic
class HeatingObjSyncRepoTesting:

    @pytest.fixture
    def dataset_ids(self):
        return [
            "dataset_1",
            "dataset_2",
            "dataset_3",
            "dataset_4",
        ]

    @pytest.fixture
    def time_tick_(self):
        return time_tick.TIME_TICK

    @pytest.fixture
    def heating_obj_df_factory(self, time_tick_):
        def factory():
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

        return factory

    def test_heating_obj_sync_repo(self, heating_obj_df_factory, repository, dataset_ids):
        heating_obj_dfs = []
        for dataset_id in dataset_ids:
            heating_obj_df = heating_obj_df_factory()
            repository.store_dataset(dataset_id, heating_obj_df)
            heating_obj_dfs.append(heating_obj_df)

        datasets_in_repository_ids = list(sorted(repository.list()))
        stored_dataset_ids = list(sorted(dataset_ids))
        assert datasets_in_repository_ids == stored_dataset_ids

        for dataset_id, original_df in zip(dataset_ids, heating_obj_dfs):
            loaded_df = repository.load_dataset(dataset_id)
            assert loaded_df.to_dict("records") == original_df.to_dict("records")
