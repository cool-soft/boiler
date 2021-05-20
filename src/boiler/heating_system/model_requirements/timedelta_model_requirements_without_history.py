from typing import Tuple

import pandas as pd

from boiler.constants import dataset_prototypes, column_names
from .abstract_model_requirements import AbstractModelRequirements


class TimedeltaModelRequirementsWithoutHistory(AbstractModelRequirements):

    def __init__(self,
                 timedelta_df: pd.DataFrame,
                 ) -> None:
        self._timedelta_df = timedelta_df.copy()

    def get_weather_start_end_timestamps(self,
                                         control_action_timestamp: pd.Timestamp
                                         ) -> Tuple[pd.Timestamp, pd.Timestamp]:
        min_timedelta = self._timedelta_df[column_names.AVG_TIMEDELTA].min()
        max_timedelta = self._timedelta_df[column_names.AVG_TIMEDELTA].max()
        return control_action_timestamp + min_timedelta, control_action_timestamp + max_timedelta

    def get_heating_states_history_timestamps(self,
                                              control_action_timestamp: pd.Timestamp
                                              ) -> pd.DataFrame:
        return dataset_prototypes.HEATING_SYSTEM_STATES_HISTORY_BORDERS.copy()
