import logging
from typing import BinaryIO

import pandas as pd

from boiler.heating_obj.io.abstract_sync_heating_obj_writer \
    import AbstractSyncHeatingObjWriter


class SyncHeatingObjCSVWriter(AbstractSyncHeatingObjWriter):

    def __init__(self,
                 encoding: str = "utf-8",
                 separator: str = ";"
                 ) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._encoding = encoding
        self._separator = separator

        self._logger.debug(f"Encoding is {encoding}")
        self._logger.debug(f"Separator is {separator}")

    def write_heating_obj_to_binary_stream(self,
                                           binary_stream: BinaryIO,
                                           heating_obj_df: pd.DataFrame) -> None:
        self._logger.debug("Storing heating object")
        heating_obj_df.to_csv(binary_stream, index=False, encoding=self._encoding, sep=self._separator)
        self._logger.debug("Heating object is stored")
