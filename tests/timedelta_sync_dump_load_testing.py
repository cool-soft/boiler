from random import random

import pytest
import pandas as pd

from boiler.constants import column_names, dataset_prototypes


# noinspection PyMethodMayBeStatic
class TimedeltaSyncDumpLoadTesting:
    row_count = 10
    min_timedelta_in_seconds = 30
    max_timedelta_in_seconds = 7200

    @pytest.fixture
    def timedelta_df(self):
        timedelta_df_ = dataset_prototypes.TIMEDELTA.copy()
        new_data = []
        for i in range(self.row_count):
            new_data.append(
                {
                    column_names.HEATING_OBJ_ID: f"obj_{i}",
                    column_names.AVG_TIMEDELTA: self._random_timedelta()
                }
            )
        timedelta_df_ = pd.concat(
            [timedelta_df_, pd.DataFrame(new_data)],
            ignore_index=True
        )
        return timedelta_df_

    def _random_timedelta(self):
        timedelta_dev = self.max_timedelta_in_seconds - self.min_timedelta_in_seconds
        timedelta_in_seconds = self.max_timedelta_in_seconds + (random() * timedelta_dev)
        return pd.Timedelta(seconds=timedelta_in_seconds)

    def test_timedelta_sync_dump_load(self, timedelta_df, dumper, loader):
        dumper.dump_timedelta(timedelta_df)
        loaded_timedelta = loader.load_timedelta()
        assert loaded_timedelta.to_dict("records") == timedelta_df.to_dict("records")
