import logging
from typing import List, Optional

import pandas as pd

from boiler.data_processing.dataset_processors.abstract_dataset_processor import AbstractDatasetProcessor


class DataFrameProcessorsGroup(AbstractDatasetProcessor):

    def __init__(self, processors: Optional[List[AbstractDatasetProcessor]] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        if processors is None:
            processors = []
        self._processors = processors.copy()

        self._logger.debug(f"Found {len(processors)} dataframe_processors")

    def set_processors(self, processors: List[AbstractDatasetProcessor]) -> None:
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
