import logging
from typing import Optional

import pandas as pd

from boiler.constants import column_names
from .weather_data_processor import HeatingObjDataProcessor
from ...utils.processing_utils import TimestampRoundAlgo


class HeatingObjDataTimestampRounder(HeatingObjDataProcessor):

    def __init__(self,
                 round_algo: Optional[TimestampRoundAlgo] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._round_algo = round_algo

        self._logger.debug(f"Round algo is {round_algo}")

    def set_round_algo(self, round_algo: TimestampRoundAlgo) -> None:
        self._logger.debug(f"Round algo is set to {round_algo}")
        self._round_algo = round_algo

    def process_heating_obj_df(
            self,
            df: pd.DataFrame,
            start_datetime: Optional[pd.Timestamp] = None,
            end_datetime: Optional[pd.Timestamp] = None,
            inplace: bool = False
    ) -> pd.DataFrame:
        self._logger.debug("Processing is requested")

        if not inplace:
            df = df.copy()
        df[column_names.TIMESTAMP] = self._round_algo.round_series(df[column_names.TIMESTAMP])
        df.drop_duplicates(column_names.TIMESTAMP, inplace=True, ignore_index=True)

        self._logger.debug("Processed")
        return df
