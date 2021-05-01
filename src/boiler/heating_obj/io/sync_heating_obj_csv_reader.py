import io
import logging
from typing import BinaryIO

import pandas as pd

from boiler.constants import column_names
from boiler.heating_obj.io.abstract_sync_heating_obj_reader import AbstractSyncHeatingObjReader


class SyncHeatingObjCSVReader(AbstractSyncHeatingObjReader):

    def __init__(self, encoding: str = "utf-8") -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._encoding = encoding

        self._logger.debug(f"Encoding is {encoding}")

    def read_heating_obj_from_binary_stream(self,
                                            binary_stream: BinaryIO) -> pd.DataFrame:
        self._logger.debug("Loading heating object")
        with io.TextIOWrapper(binary_stream, encoding=self._encoding) as text_stream:
            heating_obj_df = pd.read_csv(text_stream, parse_dates=[column_names.TIMESTAMP])
        self._logger.debug("Heating object is loaded")
        return heating_obj_df
