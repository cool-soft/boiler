import logging
from typing import BinaryIO

import pandas as pd

from boiler.heating_obj.io.abstract_sync_heating_obj_reader import AbstractSyncHeatingObjReader


class SyncHeatingObjPickleReader(AbstractSyncHeatingObjReader):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

    def read_heating_obj_from_binary_stream(self,
                                            binary_stream: BinaryIO
                                            ) -> pd.DataFrame:
        self._logger.debug("Loading heating object")
        heating_obj_df = pd.read_pickle(binary_stream)
        self._logger.debug("Heating object is loaded")
        return heating_obj_df
