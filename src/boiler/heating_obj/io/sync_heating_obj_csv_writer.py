import io
import logging
from typing import BinaryIO

import pandas as pd

from boiler.heating_obj.io.abstract_sync_heating_obj_writer \
    import AbstractSyncHeatingObjWriter


class SyncHeatingObjCSVWriter(AbstractSyncHeatingObjWriter):

    def __init__(self, encoding: str = "utf-8") -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._encoding = encoding

        self._logger.debug(f"Encoding is {encoding}")

    def write_heating_obj_to_binary_stream(self,
                                           binary_stream: BinaryIO,
                                           heating_obj_df: pd.DataFrame) -> None:
        self._logger.debug("Storing heating object")
        with io.TextIOWrapper(binary_stream, encoding=self._encoding) as text_stream:
            heating_obj_df.to_csv(text_stream, index=False)
        self._logger.debug("Heating object is stored")
