from typing import Tuple

import pandas as pd


class AbstractModelRequirements:

    def get_weather_start_end_timestamps(self,
                                         control_action_timestamp: pd.Timestamp
                                         ) -> Tuple[pd.Timestamp, pd.Timestamp]:
        raise NotImplementedError

    def get_heating_states_history_timestamps(self,
                                              control_action_timestamp: pd.Timestamp
                                              ) -> pd.DataFrame:
        raise NotImplementedError
