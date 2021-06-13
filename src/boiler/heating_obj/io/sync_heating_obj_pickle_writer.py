import pickle
from typing import BinaryIO

import pandas as pd
from boiler.logger import boiler_logger

from boiler.heating_obj.io.abstract_sync_heating_obj_writer import AbstractSyncHeatingObjWriter


class SyncHeatingObjPickleWriter(AbstractSyncHeatingObjWriter):

    def __init__(self) -> None:
        boiler_logger.debug("Creating instance")

    def write_heating_obj_to_binary_stream(self,
                                           binary_stream: BinaryIO,
                                           heating_obj_df: pd.DataFrame
                                           ) -> None:
        boiler_logger.debug("Storing heating object")
        pickle.dump(heating_obj_df, binary_stream)
