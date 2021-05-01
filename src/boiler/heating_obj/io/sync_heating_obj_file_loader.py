import logging
import os
from typing import Optional

import pandas as pd

from boiler.data_processing.beetween_filter_algorithm \
    import AbstractTimestampFilterAlgorithm, LeftClosedTimestampFilterAlgorithm
from boiler.heating_obj.io.abstract_sync_heating_obj_loader import AbstractSyncHeatingObjLoader
from boiler.heating_obj.io.abstract_sync_heating_obj_reader import AbstractSyncHeatingObjReader


class SyncHeatingObjFileLoader(AbstractSyncHeatingObjLoader):

    def __init__(self,
                 filepath: str,
                 reader: AbstractSyncHeatingObjReader,
                 timestamp_filter_algorithm: AbstractTimestampFilterAlgorithm =
                 LeftClosedTimestampFilterAlgorithm()
                 ) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)

        self._filepath = filepath
        self._reader = reader
        self._filter_algorithm = timestamp_filter_algorithm

    def load_heating_obj(self,
                         start_datetime: Optional[pd.Timestamp] = None,
                         end_datetime: Optional[pd.Timestamp] = None
                         ) -> pd.DataFrame:
        heating_object_df = self._load_from_file()
        heating_object_df = self._filter_by_timestamp(end_datetime, heating_object_df, start_datetime)
        self._logger.debug("Heating object is loaded")
        return heating_object_df

    def _filter_by_timestamp(self, end_datetime, heating_object_df, start_datetime):
        heating_object_df = self._filter_algorithm.filter_df_by_min_max_timestamp(
            heating_object_df,
            start_datetime,
            end_datetime
        )
        return heating_object_df

    def _load_from_file(self):
        filepath = os.path.abspath(self._filepath)
        self._logger.debug(f"Loading heating object from {filepath}")
        with open(filepath, mode="rb") as input_file:
            heating_object_df = self._reader.read_heating_obj_from_binary_stream(input_file)
        return heating_object_df
