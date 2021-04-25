import logging
import os
from typing import Optional

import pandas as pd

from .sync_heating_obj_binary_writer import SyncHeatingObjBinaryWriter
from .sync_heating_obj_dumper import SyncHeatingObjDumper


class SyncHeatingObjBinaryFileDumper(SyncHeatingObjDumper):

    def __init__(self,
                 filepath: Optional[str] = None,
                 writer: Optional[SyncHeatingObjBinaryWriter] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._filepath = filepath
        self._writer = writer

        self._logger.debug(f"Filepath is {filepath}")
        self._logger.debug(f"Writer is {writer}")

    def set_filepath(self, filepath: str) -> None:
        self._logger.debug(f"Filepath is set to {filepath}")
        self._filepath = filepath

    def set_writer(self, writer: SyncHeatingObjBinaryWriter) -> None:
        self._logger.debug(f"Writer is set to {writer}")
        self._writer = writer

    def dump_heating_obj(self, heating_obj_df: pd.DataFrame) -> None:
        filepath = os.path.abspath(self._filepath)
        self._logger.debug(f"Storing heating obj to {filepath}")
        with open(filepath, "wb") as binary_file:
            self._writer.write_heating_obj_to_binary_io(binary_file, heating_obj_df)
        self._logger.debug("Heating obj is stored")
