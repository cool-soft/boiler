import logging
import os
from typing import List, Optional

import pandas as pd

from .sync_heating_obj_repository_wo_transactions import SyncHeatingObjRepositoryWithoutTransactions
from ...io.sync.sync_heating_obj_text_file_dumper import SyncHeatingObjTextFileDumper
from ...io.sync.sync_heating_obj_text_file_loader import SyncHeatingObjTextFileLoader
from ...io.sync.sync_heating_obj_text_reader import SyncHeatingObjTextReader
from ...io.sync.sync_heating_obj_text_writer import SyncHeatingObjTextWriter


class SyncHeatingObjTextFilesDirRepository(SyncHeatingObjRepositoryWithoutTransactions):

    def __init__(self,
                 dir_path: Optional[str] = None,
                 filename_ext: Optional[str] = None,
                 reader: Optional[SyncHeatingObjTextReader] = None,
                 writer: Optional[SyncHeatingObjTextWriter] = None,
                 encoding="utf-8") -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._dir_path = dir_path
        self._filename_ext = filename_ext
        self._reader = reader
        self._writer = writer
        self._encoding = encoding

        self._logger.debug(f"Dir is {dir_path}")
        self._logger.debug(f"Filename ext is {filename_ext}")
        self._logger.debug(f"Reader is {reader}")
        self._logger.debug(f"Writer is {writer}")
        self._logger.debug(f"Encoding is {encoding}")

    def set_dir_path(self, dir_path: str) -> None:
        self._logger.debug(f"Dir path is set to {dir_path}")
        self._dir_path = dir_path

    def set_filename_ext(self, filename_ext: str) -> None:
        self._logger.debug(f"Filename ext is set to {filename_ext}")
        self._filename_ext = filename_ext

    def set_encoding(self, encoding) -> None:
        self._logger.debug(f"Encoding is set to {encoding}")
        self._encoding = encoding

    def set_reader(self, reader: SyncHeatingObjTextReader) -> None:
        self._logger.debug(f"Reader is set to {reader}")
        self._reader = reader

    def set_writer(self, writer: SyncHeatingObjTextWriter) -> None:
        self._logger.debug(f"Writer is set to {writer}")
        self._writer = writer

    def list(self) -> List[str]:
        self._logger.debug("Requested listing of repository")
        heating_obj_filenames = []
        for filename_with_ext in os.listdir(self._dir_path):
            filename, ext = os.path.splitext(filename_with_ext)
            if ext == self._filename_ext:
                path = f"{self._dir_path}/{filename_with_ext}"
                if os.path.isfile(path):
                    heating_obj_filenames.append(filename)
        self._logger.debug(f"Found {len(heating_obj_filenames)} heating objects")
        return heating_obj_filenames

    def get_dataset(self,
                    dataset_id: str,
                    start_datetime: Optional[pd.Timestamp] = None,
                    end_datetime: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        dataset_path = os.path.abspath(f"{self._dir_path}/{dataset_id}{self._filename_ext}")
        logging.debug(f"Loading {dataset_path} from {start_datetime} to {end_datetime}")
        loader = SyncHeatingObjTextFileLoader(reader=self._reader)
        heating_obj_df = loader.load_heating_obj(start_datetime, end_datetime)
        return heating_obj_df

    def set_dataset(self, dataset_id: str, heating_obj_df: pd.DataFrame) -> None:
        dataset_path = os.path.abspath(f"{self._dir_path}/{dataset_id}{self._filename_ext}")
        logging.debug(f"Saving {dataset_path}")
        dumper = SyncHeatingObjTextFileDumper(writer=self._writer)
        dumper.dump_heating_obj(heating_obj_df)

    def del_dataset(self, dataset_id: str) -> None:
        dataset_path = os.path.abspath(f"{self._dir_path}/{dataset_id}{self._filename_ext}")
        self._logger.debug(f"Deleting dataset {dataset_id} from file {dataset_path}")
        os.remove(dataset_path)