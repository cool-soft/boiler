import logging
from typing import Optional

import pandas as pd

from boiler.constants import dataset_prototypes
from boiler.data_processing.beetween_filter_algorithm import AbstractTimestampFilterAlgorithm, \
    LeftClosedTimestampFilterAlgorithm
from boiler.heating_obj.io.sync.sync_heating_obj_dumper import SyncHeatingObjDumper
from boiler.heating_obj.io.sync.sync_heating_obj_loader import SyncHeatingObjLoader


class SyncHeatingObjInMemoryDumperLoader(SyncHeatingObjDumper, SyncHeatingObjLoader):

    def __init__(self,
                 filter_algorithm: AbstractTimestampFilterAlgorithm =
                 LeftClosedTimestampFilterAlgorithm()) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)

        self._storage = dataset_prototypes.HEATING_OBJ.copy()
        self._filter_algorithm = filter_algorithm

    def load_heating_obj(self,
                         start_datetime: Optional[pd.Timestamp] = None,
                         end_datetime: Optional[pd.Timestamp] = None
                         ) -> pd.DataFrame:
        self._logger.debug("Requested heating object")
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
        self._logger.debug("Storing heating obj")
        self._storage = heating_obj_df.copy()
