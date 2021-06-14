from typing import BinaryIO

import pandas as pd

from boiler.heating_obj.io.abstract_sync_heating_obj_writer \
    import AbstractSyncHeatingObjWriter
from boiler.logger import logger


class SyncHeatingObjCSVWriter(AbstractSyncHeatingObjWriter):

    def __init__(self,
                 encoding: str = "utf-8",
                 separator: str = ";"
                 ) -> None:
        self._encoding = encoding
        self._separator = separator

        logger.debug(
            f"Creating instance:"
            f"encoding: {encoding}"
            f"separator: {separator}"
        )

    def write_heating_obj_to_binary_stream(self,
                                           binary_stream: BinaryIO,
                                           heating_obj_df: pd.DataFrame
                                           ) -> None:
        logger.debug(f"Storing heating object; len = {len(heating_obj_df)}")
        heating_obj_df.to_csv(binary_stream, index=False, encoding=self._encoding, sep=self._separator)
