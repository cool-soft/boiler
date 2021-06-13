import pandas as pd
from boiler.logger import boiler_logger

from boiler.constants import dataset_prototypes
from boiler.timedelta.io.abstract_sync_timedelta_dumper import AbstractSyncTimedeltaDumper
from boiler.timedelta.io.abstract_sync_timedelta_loader import AbstractSyncTimedeltaLoader


class SyncTimedeltaInMemoryDumperLoader(AbstractSyncTimedeltaDumper, AbstractSyncTimedeltaLoader):

    def __init__(self) -> None:
        boiler_logger.debug("Creating instance")

        self._storage = dataset_prototypes.TIMEDELTA.copy()

    def load_timedelta(self) -> pd.DataFrame:
        boiler_logger.debug("Requested timedelta")
        timedelta_df = self._storage.copy()
        return timedelta_df

    def dump_timedelta(self, timedelta_df: pd.DataFrame) -> None:
        boiler_logger.debug("Storing timedelta")
        self._storage = timedelta_df.copy()
