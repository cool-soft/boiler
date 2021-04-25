import logging
from typing import TextIO

import pandas as pd

from .sync_heating_obj_text_reader import SyncHeatingObjTextReader
from boiler.constants import column_names


class SyncHeatingObjCSVReader(SyncHeatingObjTextReader):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

    def read_heating_obj_from_text_io(self, text_io: TextIO) -> pd.DataFrame:
        self._logger.debug("Loading heating object")
        heating_obj_df = pd.read_csv(text_io, parse_dates=[column_names.TIMESTAMP])
        self._logger.debug("Heating object is loaded")
        return heating_obj_df
