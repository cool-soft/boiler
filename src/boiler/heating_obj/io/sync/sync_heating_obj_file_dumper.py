import logging
import os
from typing import Optional

import pandas as pd

from .sync_heating_obj_dumper import SyncHeatingObjDumper
from .sync_heating_obj_writer import SyncHeatingObjWriter


class SyncHeatingObjFileDumper(SyncHeatingObjDumper):

    def __init__(self,
                 filepath: Optional[str] = None,
                 writer: Optional[SyncHeatingObjWriter] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._filepath = filepath
        self._writer = writer

        self._logger.debug(f"Filepath is {filepath}")
        self._logger.debug(f"Writer is {writer}")

    def set_filepath(self, filepath: str) -> None:
        self._logger.debug(f"Filepath is set to {filepath}")
        self._filepath = filepath

    def set_writer(self, writer: SyncHeatingObjWriter) -> None:
        self._logger.debug(f"Writer is set to {writer}")
        self._writer = writer

    def dump_heating_obj(self, heating_obj_df: pd.DataFrame) -> None:
        filepath = os.path.abspath(self._filepath)
        self._logger.debug(f"Storing heating object to {filepath}")
        with open(filepath, mode="wb") as output_file:
            self._writer.write_heating_obj_to_binary_stream(output_file, heating_obj_df)
        self._logger.debug("Heating object is stored")
