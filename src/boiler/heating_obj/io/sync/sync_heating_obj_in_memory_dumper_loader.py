import logging
from typing import Optional

import pandas as pd

from boiler.constants import column_names
from boiler.data_processing.processing_algo.processing import filter_by_timestamp_closed
from boiler.heating_obj.io.sync.sync_heating_obj_dumper import SyncHeatingObjDumper
from boiler.heating_obj.io.sync.sync_heating_obj_loader import SyncHeatingObjLoader


class SyncHeatingObjInMemoryDumperLoader(SyncHeatingObjDumper, SyncHeatingObjLoader):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        # TODO: вынести создание пустого DataFrame с заданными колонками куда-нибудь
        self._storage = pd.DataFrame(
            columns=(column_names.TIMESTAMP,
                     column_names.WEATHER_TEMP)
        )

    def load_heating_obj(self,
                         start_datetime: Optional[pd.Timestamp] = None,
                         end_datetime: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        self._logger.debug("Requested heating object")
        heating_object_df = self._storage.copy()
        heating_object_df = filter_by_timestamp_closed(heating_object_df, start_datetime, end_datetime)
        return heating_object_df

    def dump_heating_obj(self, heating_obj_df: pd.DataFrame) -> None:
        self._logger.debug("Storing heating obj")
        self._storage = heating_obj_df.copy()
