from typing import BinaryIO

import pandas as pd
from boiler.logger import boiler_logger

from boiler.constants import column_names
from boiler.heating_obj.io.abstract_sync_heating_obj_reader import AbstractSyncHeatingObjReader


class SyncHeatingObjCSVReader(AbstractSyncHeatingObjReader):

    def __init__(self,
                 encoding: str = "utf-8",
                 separator: str = ";"
                 ) -> None:
        self._encoding = encoding
        self._separator = separator

        boiler_logger.debug(
            f"Creating instance:"
            f"encoding: {encoding}"
            f"separator: {separator}"
        )

    def read_heating_obj_from_binary_stream(self,
                                            binary_stream: BinaryIO
                                            ) -> pd.DataFrame:
        boiler_logger.debug("Loading heating object")
        heating_obj_df = pd.read_csv(
            binary_stream,
            parse_dates=[column_names.TIMESTAMP],
            sep=self._separator,
            encoding=self._encoding
        )
        return heating_obj_df
