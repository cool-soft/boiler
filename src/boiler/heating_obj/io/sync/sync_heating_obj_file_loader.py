import logging
import os
from typing import Optional

import pandas as pd

from boiler.utils.processing_utils import filter_by_timestamp_closed
from boiler.heating_obj.io.sync.sync_heating_obj_reader import SyncHeatingObjReader
from boiler.heating_obj.io.sync.sync_heating_obj_loader import SyncHeatingObjLoader


class SyncHeatingObjFileLoader(SyncHeatingObjLoader):

    def __init__(self,
                 filepath: Optional[str] = None,
                 reader: Optional[SyncHeatingObjReader] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._filepath = filepath
        self._reader = reader

        self._logger.debug(f"Filepath is {filepath}")
        self._logger.debug(f"Reader is {reader}")

    def set_filepath(self, filepath: str):
        self._logger.debug(f"Set filepath to {filepath}")
        self._filepath = filepath

    def set_reader(self, reader: SyncHeatingObjReader):
        self._logger.debug(f"Reader is set to {reader}")
        self._reader = reader

    def load_heating_obj(self,
                         start_datetime: Optional[pd.Timestamp] = None,
                         end_datetime: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        filepath = os.path.abspath(self._filepath)
        self._logger.debug(f"Loading heating object from {filepath}")
        with open(filepath, mode="rb") as input_file:
            heating_object_df = self._reader.read_heating_obj_from_binary_stream(input_file)
        heating_object_df = filter_by_timestamp_closed(heating_object_df, start_datetime, end_datetime)
        self._logger.debug("Heating object is loaded")
        return heating_object_df
