from random import random

import pandas as pd
import pytest
from dateutil.tz import tzlocal

from boiler.constants import column_names, time_tick, dataset_prototypes
from boiler.data_processing.processing_algo.beetween_filter_algorithm import FullClosedBetweenFilterAlgorithm


# noinspection PyMethodMayBeStatic
class HeatingObjSyncRepoTesting:

    dataset_ids = [
            "dataset_1",
            "dataset_2",
            "dataset_3",
            "dataset_4",
        ]

    time_tick_ = time_tick.TIME_TICK
    heating_obj_df_len = 5

    @pytest.fixture
    def filter_algorithm(self):
        return FullClosedBetweenFilterAlgorithm(column_name=column_names.TIMESTAMP)

    @pytest.fixture
    def heating_obj_df_factory(self):
        def factory():
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

        return factory

    def test_heating_obj_sync_repo(self, heating_obj_df_factory, repository):
        heating_obj_dfs = []
        for dataset_id in self.dataset_ids:
            heating_obj_df = heating_obj_df_factory()
            repository.store_dataset(dataset_id, heating_obj_df)
            heating_obj_dfs.append(heating_obj_df)

        datasets_in_repository_ids = list(sorted(repository.list()))
        stored_dataset_ids = list(sorted(self.dataset_ids))
        assert datasets_in_repository_ids == stored_dataset_ids

        for dataset_id, original_df in zip(self.dataset_ids, heating_obj_dfs):
            loaded_df = repository.load_dataset(dataset_id)
            assert loaded_df.to_dict("records") == original_df.to_dict("records")
