import logging
from typing import Optional

import pandas as pd

from boiler.constants import dataset_prototypes, column_names
from boiler.data_processing.beetween_filter_algorithm \
    import LeftClosedBetweenFilterAlgorithm, AbstractBetweenFilterAlgorithm
from boiler.heating_obj.io.sync.sync_heating_obj_dumper import SyncHeatingObjDumper
from boiler.heating_obj.io.sync.sync_heating_obj_loader import SyncHeatingObjLoader


class SyncHeatingObjInMemoryDumperLoader(SyncHeatingObjDumper, SyncHeatingObjLoader):

    def __init__(self,
                 filter_algorithm: Optional[AbstractBetweenFilterAlgorithm] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._storage = dataset_prototypes.HEATING_OBJ.copy()
        if filter_algorithm is None:
            filter_algorithm = LeftClosedBetweenFilterAlgorithm(column_name=column_names.TIMESTAMP)
        self._filter_algorithm = filter_algorithm

    def set_filter_algorithm(self, algorithm: AbstractBetweenFilterAlgorithm) -> None:
        self._logger.debug(f"Filter algorithm is set to {algorithm}")
        self._filter_algorithm = algorithm

    def load_heating_obj(self,
                         start_datetime: Optional[pd.Timestamp] = None,
                         end_datetime: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        self._logger.debug("Requested heating object")
        heating_object_df = self._storage.copy()
        heating_object_df = self._filter_algorithm.filter_df_by_min_max_values(
            heating_object_df,
            start_datetime,
            end_datetime
        )
        return heating_object_df

    def dump_heating_obj(self, heating_obj_df: pd.DataFrame) -> None:
        self._logger.debug("Storing heating obj")
        self._storage = heating_obj_df.copy()
