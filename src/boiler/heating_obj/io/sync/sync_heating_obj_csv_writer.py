import logging
from typing import TextIO

import pandas as pd

from .sync_heating_obj_text_writer import SyncHeatingObjTextWriter


class SyncHeatingObjCSVWriter(SyncHeatingObjTextWriter):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

    def write_heating_obj_to_text_io(self,
                                     text_io: TextIO,
                                     heating_obj_df: pd.DataFrame) -> None:
        self._logger.debug("Storing heating object")
        heating_obj_df.to_csv(text_io, index=False)
        self._logger.debug("Heating object is stored")
