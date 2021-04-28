import logging
from typing import List, Optional

import pandas as pd

from boiler.dataset_processing.dataset_processor import DatasetProcessor


class DataFrameProcessorsGroup(DatasetProcessor):

    def __init__(self, processors: Optional[List[DatasetProcessor]] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        if processors is None:
            processors = []
        self._processors = processors.copy()

        self._logger.debug(f"Found {len(processors)} dataframe_processors")

    def set_processors(self, processors: List[DatasetProcessor]) -> None:
        self._logger.debug("Processors is set")
        self._processors = processors.copy()
        self._logger.debug(f"Found {len(processors)} dataframe_processors")

    def process_df(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.debug("Requested weather_df processing")
        df = df.copy()
        for processor in self._processors:
            df = processor.process_df(df)
        self._logger.debug("Processed")
        return df
