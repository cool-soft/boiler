import logging
import os
from typing import List, Optional

import pandas as pd

from .sync_heating_obj_repository_wo_transactions import SyncHeatingObjRepositoryWithoutTransactions
from boiler.heating_obj.io.sync.sync_heating_obj_file_dumper import SyncHeatingObjFileDumper
from boiler.heating_obj.io.sync.sync_heating_obj_file_loader import SyncHeatingObjFileLoader
from boiler.heating_obj.io.sync.sync_heating_obj_reader import SyncHeatingObjReader
from boiler.heating_obj.io.sync.sync_heating_obj_writer import SyncHeatingObjWriter


class SyncHeatingObjDirRepository(SyncHeatingObjRepositoryWithoutTransactions):

    def __init__(self,
                 dir_path: Optional[str] = None,
                 filename_ext: Optional[str] = None,
                 reader: Optional[SyncHeatingObjReader] = None,
                 writer: Optional[SyncHeatingObjWriter] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._dir_path = dir_path
        self._filename_ext = filename_ext
        self._reader = reader
        self._writer = writer

        self._logger.debug(f"Dir is {dir_path}")
        self._logger.debug(f"Filename ext is {filename_ext}")
        self._logger.debug(f"Reader is {reader}")
        self._logger.debug(f"Writer is {writer}")

    def set_dir_path(self, dir_path: str) -> None:
        self._logger.debug(f"Dir path is set to {dir_path}")
        self._dir_path = dir_path

    def set_filename_ext(self, filename_ext: str) -> None:
        self._logger.debug(f"Filename ext is set to {filename_ext}")
        self._filename_ext = filename_ext

    def set_reader(self, reader: SyncHeatingObjReader) -> None:
        self._logger.debug(f"Reader is set to {reader}")
        self._reader = reader

    def set_writer(self, writer: SyncHeatingObjWriter) -> None:
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

    def load_dataset(self,
                     dataset_id: str,
                     start_datetime: Optional[pd.Timestamp] = None,
                     end_datetime: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        dataset_path = os.path.abspath(f"{self._dir_path}/{dataset_id}{self._filename_ext}")
        logging.debug(f"Loading {dataset_path} from {start_datetime} to {end_datetime}")
        loader = SyncHeatingObjFileLoader(
            reader=self._reader,
            filepath=dataset_path
        )
        heating_obj_df = loader.load_heating_obj(start_datetime, end_datetime)
        return heating_obj_df

    def store_dataset(self, dataset_id: str, heating_obj_df: pd.DataFrame) -> None:
        dataset_path = os.path.abspath(f"{self._dir_path}/{dataset_id}{self._filename_ext}")
        logging.debug(f"Saving {dataset_path}")
        dumper = SyncHeatingObjFileDumper(
            writer=self._writer,
            filepath=dataset_path
        )
        dumper.dump_heating_obj(heating_obj_df)

    def del_dataset(self, dataset_id: str) -> None:
        dataset_path = os.path.abspath(f"{self._dir_path}/{dataset_id}{self._filename_ext}")
        self._logger.debug(f"Deleting dataset {dataset_id} from file {dataset_path}")
        os.remove(dataset_path)
