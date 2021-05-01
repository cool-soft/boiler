import logging
import pickle
from typing import BinaryIO

import pandas as pd

from boiler.heating_obj.io.abstract_sync_heating_obj_writer import AbstractSyncHeatingObjWriter


class SyncHeatingObjPickleWriter(AbstractSyncHeatingObjWriter):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

    def write_heating_obj_to_binary_stream(self,
                                           binary_stream: BinaryIO,
                                           heating_obj_df: pd.DataFrame) -> None:
        self._logger.debug("Storing heating object")
        pickle.dump(heating_obj_df, binary_stream)
        self._logger.debug("Heating object is stored")
