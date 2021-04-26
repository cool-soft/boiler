import logging
from datetime import timedelta
from typing import List, Optional

import pandas as pd

from boiler.constants import column_names
from boiler.heating_obj.processors.heating_obj_data_processor import HeatingObjDataProcessor


class HeatingObjDataLinearInterpolator(HeatingObjDataProcessor):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance of the provider")

        self._columns_to_interpolate = (
            column_names.FORWARD_PIPE_COOLANT_TEMP,
            column_names.BACKWARD_PIPE_COOLANT_TEMP,
            column_names.FORWARD_PIPE_COOLANT_VOLUME,
            column_names.BACKWARD_PIPE_COOLANT_VOLUME,
            column_names.FORWARD_PIPE_COOLANT_PRESSURE,
            column_names.BACKWARD_PIPE_COOLANT_PRESSURE
        )
        self._interpolation_step = timedelta(seconds=180)

    def set_columns_to_interpolate(self, columns: List[str]) -> None:
        self._columns_to_interpolate = columns

    def set_interpolation_step(self, interpolation_step: pd.Timedelta) -> None:
        self._interpolation_step = interpolation_step

    def process_heating_obj_df(self,
                               heating_obj_df: pd.DataFrame,
                               start_datetime: Optional[pd.Timestamp] = None,
                               end_datetime: Optional[pd.Timestamp] = None,
                               inplace: bool = False) -> pd.DataFrame:
        self._logger.debug("Interpolating is requested")
        if not inplace:
            heating_obj_df = heating_obj_df.copy()
        heating_obj_df.sort_values(by=column_names.TIMESTAMP, ignore_index=True, inplace=True)
        heating_obj_df = self._interpolate_passes_of_datetime(heating_obj_df)
        self._interpolate_passes_of_data(heating_obj_df)
        self._logger.debug("Interpolated")
        return heating_obj_df

    def _interpolate_passes_of_datetime(self, heating_obj_df: pd.DataFrame):
        self._logger.debug("Interpolating passes of datetime")

        datetime_to_insert = []
        previous_datetime = None
        for timestamp in heating_obj_df[column_names.TIMESTAMP].to_list():
            if previous_datetime is None:
                previous_datetime = timestamp
                continue
            next_datetime = timestamp

            current_datetime = previous_datetime + self._interpolation_step
            while current_datetime < next_datetime:
                datetime_to_insert.append({
                    column_names.TIMESTAMP: current_datetime
                })
                current_datetime += self._interpolation_step

            previous_datetime = next_datetime

        heating_obj_df = heating_obj_df.append(datetime_to_insert, ignore_index=True)
        heating_obj_df.sort_values(by=column_names.TIMESTAMP, ignore_index=True, inplace=True)

        return heating_obj_df

    def _interpolate_passes_of_data(self, heating_obj_df: pd.DataFrame) -> None:
        self._logger.debug("Interpolating passes of data")
        for column_to_interpolate in self._columns_to_interpolate:
            heating_obj_df[column_to_interpolate] = pd.to_numeric(heating_obj_df[column_to_interpolate], downcast="float")
            heating_obj_df[column_to_interpolate].interpolate(inplace=True)
