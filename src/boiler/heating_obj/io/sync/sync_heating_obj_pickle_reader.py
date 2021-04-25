import logging
from typing import BinaryIO
import pickle

import pandas as pd

from .sync_heating_obj_binary_reader import SyncHeatingObjBinaryReader


class SyncHeatingObjPickleReader(SyncHeatingObjBinaryReader):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

    def read_heating_obj_from_binary_io(self, binary_io: BinaryIO) -> pd.DataFrame:
        self._logger.debug("Loading heating object")
        heating_obj_df = pickle.load(binary_io)
        self._logger.debug("Heating object is loaded")
        return heating_obj_df
