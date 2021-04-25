import logging
import os
from typing import Optional

import pandas as pd

from .sync_heating_obj_dumper import SyncHeatingObjDumper
from .sync_heating_obj_text_writer import SyncHeatingObjTextWriter


class SyncHeatingObjTextFileDumper(SyncHeatingObjDumper):

    def __init__(self,
                 filepath: Optional[str] = None,
                 writer: Optional[SyncHeatingObjTextWriter] = None,
                 encoding="utf-8") -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._filepath = filepath
        self._writer = writer
        self._encoding = encoding

        self._logger.debug(f"Filepath is {filepath}")
        self._logger.debug(f"Writer is {writer}")
        self._logger.debug(f"Encoding is {encoding}")

    def set_filepath(self, filepath: str) -> None:
        self._logger.debug(f"Filepath is set to {filepath}")
        self._filepath = filepath

    def set_writer(self, writer: SyncHeatingObjTextWriter) -> None:
        self._logger.debug(f"Writer is set to {writer}")
        self._writer = writer

    def set_encoding(self, encoding: str) -> None:
        self._logger.debug(f"Encoding is set to {encoding}")
        self._encoding = encoding

    def dump_heating_obj(self, heating_obj_df: pd.DataFrame) -> None:
        filepath = os.path.abspath(self._filepath)
        self._logger.debug(f"Storing heating object to {filepath}")
        with open(filepath, mode="w", encoding=self._encoding) as output_file:
            self._writer.write_heating_obj_to_text_io(output_file, heating_obj_df)
        self._logger.debug("Heating object is stored")
