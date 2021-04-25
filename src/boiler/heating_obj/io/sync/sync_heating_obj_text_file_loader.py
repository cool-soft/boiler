import logging
import os
from typing import Optional

import pandas as pd

from boiler.parsing_utils.utils import filter_by_timestamp_closed
from .sync_heating_obj_text_reader import SyncHeatingObjTextReader
from .sync_heating_obj_loader import SyncHeatingObjLoader


class SyncHeatingObjTextFileLoader(SyncHeatingObjLoader):

    def __init__(self,
                 filepath: Optional[str] = None,
                 reader: Optional[SyncHeatingObjTextReader] = None,
                 encoding="utf-8") -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._filepath = filepath
        self._reader = reader
        self._encoding = encoding

        self._logger.debug(f"Filepath is {filepath}")
        self._logger.debug(f"Reader is {reader}")
        self._logger.debug(f"Encoding is {encoding}")

    def set_filepath(self, filepath: str):
        self._logger.debug(f"Set filepath to {filepath}")
        self._filepath = filepath

    def set_encoding(self, encoding: str) -> None:
        self._logger.debug(f"Encoding is set to {encoding}")
        self._encoding = encoding

    def set_reader(self, reader: SyncHeatingObjTextReader):
        self._logger.debug(f"Reader is set to {reader}")
        self._reader = reader

    def load_heating_obj(self,
                         start_datetime: Optional[pd.Timestamp] = None,
                         end_datetime: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        filepath = os.path.abspath(self._filepath)
        self._logger.debug(f"Loading heating object from {filepath}")
        with open(filepath, mode="r", encoding=self._encoding) as text_io:
            heating_object_df = self._reader.read_heating_obj_from_text_io(text_io)
        heating_object_df = filter_by_timestamp_closed(heating_object_df, start_datetime, end_datetime)
        self._logger.debug("Heating object is loaded")
        return heating_object_df