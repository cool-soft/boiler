import os

import pandas as pd
from boiler.logger import boiler_logger

from boiler.heating_obj.io.abstract_sync_heating_obj_dumper import AbstractSyncHeatingObjDumper
from boiler.heating_obj.io.abstract_sync_heating_obj_writer import AbstractSyncHeatingObjWriter


class SyncHeatingObjFileDumper(AbstractSyncHeatingObjDumper):

    def __init__(self,
                 filepath: str,
                 writer: AbstractSyncHeatingObjWriter
                 ) -> None:
        self._filepath = filepath
        self._writer = writer
        boiler_logger.debug(
            f"Creating instance:"
            f"filepath: {filepath}"
            f"writer: {writer}"
        )

    def dump_heating_obj(self, heating_obj_df: pd.DataFrame) -> None:
        filepath = os.path.abspath(self._filepath)
        boiler_logger.debug(f"Storing heating object to {filepath}")
        with open(filepath, mode="wb") as output_file:
            self._writer.write_heating_obj_to_binary_stream(output_file, heating_obj_df)
