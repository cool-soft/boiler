from typing import Optional

import pandas as pd
from boiler.logging import logger

from boiler.constants import dataset_prototypes
from boiler.data_processing.beetween_filter_algorithm import AbstractTimestampFilterAlgorithm, \
    LeftClosedTimestampFilterAlgorithm
from boiler.heating_obj.io.abstract_sync_heating_obj_dumper import AbstractSyncHeatingObjDumper
from boiler.heating_obj.io.abstract_sync_heating_obj_loader import AbstractSyncHeatingObjLoader


class SyncHeatingObjInMemoryDumperLoader(AbstractSyncHeatingObjDumper, AbstractSyncHeatingObjLoader):

    def __init__(self,
                 filter_algorithm: AbstractTimestampFilterAlgorithm =
                 LeftClosedTimestampFilterAlgorithm()
                 ) -> None:
        self._storage = dataset_prototypes.HEATING_OBJ.copy()
        self._filter_algorithm = filter_algorithm
        logger.debug(
            f"Creating instance:"
            f"filter algorithm: {filter_algorithm}"
        )

    def load_heating_obj(self,
                         start_datetime: Optional[pd.Timestamp] = None,
                         end_datetime: Optional[pd.Timestamp] = None
                         ) -> pd.DataFrame:
        logger.debug(f"Loading for {start_datetime}, {end_datetime}")
        heating_object_df = self._load_from_storage()
        heating_object_df = self._filter_by_timestamp(end_datetime, heating_object_df, start_datetime)
        return heating_object_df

    def _load_from_storage(self):
        heating_object_df = self._storage.copy()
        return heating_object_df

    def _filter_by_timestamp(self, end_datetime, heating_object_df, start_datetime):
        heating_object_df = self._filter_algorithm.filter_df_by_min_max_timestamp(
            heating_object_df,
            start_datetime,
            end_datetime
        )
        return heating_object_df

    def dump_heating_obj(self, heating_obj_df: pd.DataFrame) -> None:
        logger.debug(f"Storing heating obj; df len = {len(heating_obj_df)}")
        self._storage = heating_obj_df.copy()
