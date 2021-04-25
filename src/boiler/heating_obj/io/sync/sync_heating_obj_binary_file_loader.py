import logging
import os
from typing import Optional

import pandas as pd

from .sync_heating_obj_binary_reader import SyncHeatingObjBinaryReader
from .sync_heating_obj_loader import SyncHeatingObjLoader


class SyncHeatingObjBinaryFileLoader(SyncHeatingObjLoader):

    def __init__(self,
                 filepath: Optional[str] = None,
                 reader: Optional[SyncHeatingObjBinaryReader] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._filepath = filepath
        self._reader = reader

        self._logger.debug(f"Filepath is {filepath}")
        self._logger.debug(f"Reader is {reader}")

    def set_filepath(self, filepath: str) -> None:
        self._logger.debug(f"Filepath is set to {filepath}")
        self._filepath = filepath

    def set_reader(self, reader: SyncHeatingObjBinaryReader) -> None:
        self._logger.debug(f"Reader is set to {reader}")
        self._reader = reader

    def load_heating_obj(self,
                         start_datetime: Optional[pd.Timestamp] = None,
                         end_datetime: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        filepath = os.path.abspath(self._filepath)
        self._logger.debug(f"Loading heating object from {filepath}")
        with open(filepath, "rb") as binary_file:
            heating_obj_df = self._reader.read_heating_obj_from_binary_io(binary_file)
        self._logger.debug("Heating object is loaded")
        return heating_obj_df
