import logging
from typing import List, Optional

import pandas as pd

from boiler.heating_obj.processors.heating_obj_data_processor import HeatingObjDataProcessor


class HeatingObjDataProcessorsGroup(HeatingObjDataProcessor):

    def __init__(self,
                 processors: Optional[List[HeatingObjDataProcessor]] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        if processors is None:
            processors = []
        self._processors = processors.copy()

        self._logger.debug(f"Found {len(processors)} processors")

    def set_processors(self, processors: List[HeatingObjDataProcessor]) -> None:
        self._logger.debug("Processors is set")
        self._processors = processors.copy()
        self._logger.debug(f"Found {len(processors)} processors")

    def process_heating_obj_df(self,
                               heating_obj_df: pd.DataFrame,
                               start_datetime: Optional[pd.Timestamp] = None,
                               end_datetime: Optional[pd.Timestamp] = None,
                               inplace: bool = False) -> pd.DataFrame:
        self._logger.debug("Requested weather_df processing")

        for processor in self._processors:
            heating_obj_df = processor.process_heating_obj_df(
                heating_obj_df,
                start_datetime,
                end_datetime,
                inplace
            )

        self._logger.debug("Processed")
        return heating_obj_df
