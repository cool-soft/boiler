import logging
from typing import BinaryIO
import pickle

import pandas as pd

from .sync_heating_obj_binary_writer import SyncHeatingObjBinaryWriter


class SyncHeatingObjPickleWriter(SyncHeatingObjBinaryWriter):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

    def write_heating_obj_to_binary_io(self, binary_io: BinaryIO, heating_obj_df: pd.DataFrame) -> None:
        self._logger.debug("Storing heating object")
        pickle.dump(heating_obj_df, binary_io)
        self._logger.debug("Heating object is stored")
