import logging
from typing import List, Optional

import pandas as pd

from boiler.constants import column_names
from boiler.utils.processing_utils import TimestampRoundAlgo
from boiler.heating_obj.processors.heating_obj_data_processor import HeatingObjDataProcessor


class HeatingObjDataBorderComplementary(HeatingObjDataProcessor):

    def __init__(self,
                 round_algo: Optional[TimestampRoundAlgo] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance of the provider")

        self._columns_to_complementary = (
            column_names.FORWARD_PIPE_COOLANT_TEMP,
            column_names.BACKWARD_PIPE_COOLANT_TEMP,
            column_names.FORWARD_PIPE_COOLANT_VOLUME,
            column_names.BACKWARD_PIPE_COOLANT_VOLUME,
            column_names.FORWARD_PIPE_COOLANT_PRESSURE,
            column_names.BACKWARD_PIPE_COOLANT_PRESSURE
        )
        self._round_algo = round_algo

        self._logger.debug(f"Round algo {round_algo}")

    def set_columns_to_complementary(self, columns: List[str]) -> None:
        self._columns_to_complementary = columns

    def set_round_algo(self, round_algo: TimestampRoundAlgo) -> None:
        self._logger.debug(f"Round algo is set to {round_algo}")
        self._round_algo = round_algo

    def process_heating_obj_df(self,
                               heating_obj_df: pd.DataFrame,
                               start_datetime: Optional[pd.Timestamp] = None,
                               end_datetime: Optional[pd.Timestamp] = None,
                               inplace: bool = False) -> pd.DataFrame:
        self._logger.debug("Interpolating is requested")
        if not inplace:
            heating_obj_df = heating_obj_df.copy()
        heating_obj_df = self._interpolate_border_datetime(heating_obj_df, start_datetime, end_datetime)
        self._interpolate_border_data(heating_obj_df)
        self._logger.debug("Interpolated")
        return heating_obj_df

    def _interpolate_border_datetime(self,
                                     heating_obj_df: pd.DataFrame,
                                     start_datetime: pd.Timestamp,
                                     end_datetime: pd.Timestamp) -> pd.DataFrame:
        self._logger.debug("Interpolating border datetime values")

        if start_datetime is not None:
            start_datetime = self._round_algo.round_value(start_datetime)
            first_datetime_idx = heating_obj_df[column_names.TIMESTAMP].idxmin()
            first_row = heating_obj_df.loc[first_datetime_idx]
            first_datetime = first_row[column_names.TIMESTAMP]
            if first_datetime > start_datetime:
                heating_obj_df = heating_obj_df.append({column_names.TIMESTAMP: start_datetime}, ignore_index=True)

        if end_datetime is not None:
            end_datetime = self._round_algo.round_value(end_datetime)
            last_datetime_idx = heating_obj_df[column_names.TIMESTAMP].idxmax()
            last_row = heating_obj_df.loc[last_datetime_idx]
            last_datetime = last_row[column_names.TIMESTAMP]
            if last_datetime < end_datetime:
                heating_obj_df = heating_obj_df.append({column_names.TIMESTAMP: end_datetime}, ignore_index=True)

        return heating_obj_df

    def _interpolate_border_data(self, heating_obj_df: pd.DataFrame) -> None:
        self._logger.debug("Interpolating border data values")

        first_datetime_index = heating_obj_df[column_names.TIMESTAMP].idxmin()
        last_datetime_index = heating_obj_df[column_names.TIMESTAMP].idxmax()

        for column_name in self._columns_to_complementary:
            first_valid_index = heating_obj_df[column_name].first_valid_index()
            if first_valid_index != first_datetime_index:
                first_valid_value = heating_obj_df.loc[first_valid_index, column_name]
                heating_obj_df.loc[first_datetime_index, column_name] = first_valid_value

            last_valid_index = heating_obj_df[column_name].last_valid_index()
            if last_valid_index != last_datetime_index:
                last_valid_value = heating_obj_df.loc[last_valid_index, column_name]
                heating_obj_df.loc[last_datetime_index, column_name] = last_valid_value
