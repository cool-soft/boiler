import logging

import pandas as pd

from boiler.constants import dataset_prototypes
from boiler.time_delta.io.abstract_sync_timedelta_dumper import AbstractSyncTimedeltaDumper
from boiler.time_delta.io.abstract_sync_timedelta_loader import AbstractSyncTimedeltaLoader


class SyncTimedeltaInMemoryDumperLoader(AbstractSyncTimedeltaDumper, AbstractSyncTimedeltaLoader):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._storage = dataset_prototypes.TIMEDELTA.copy()

    def load_timedelta(self) -> pd.DataFrame:
        self._logger.debug("Requested timedelta")
        timedelta_df = self._storage.copy()
        return timedelta_df

    def dump_timedelta(self, timedelta_df: pd.DataFrame) -> None:
        self._logger.debug("Storing timedelta")
        self._storage = timedelta_df.copy()
