from typing import BinaryIO

import pandas as pd
from boiler.logging import logger

from boiler.heating_obj.io.abstract_sync_heating_obj_reader import AbstractSyncHeatingObjReader


class SyncHeatingObjPickleReader(AbstractSyncHeatingObjReader):

    def __init__(self) -> None:
        logger.debug("Creating instance")

    def read_heating_obj_from_binary_stream(self,
                                            binary_stream: BinaryIO
                                            ) -> pd.DataFrame:
        logger.debug("Loading heating object")
        heating_obj_df = pd.read_pickle(binary_stream)
        return heating_obj_df
