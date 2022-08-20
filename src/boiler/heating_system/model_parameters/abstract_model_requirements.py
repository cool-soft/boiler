import pandas as pd


class AbstractModelParameters:

    def get_min_heating_system_lag(self) -> pd.Timedelta:
        raise NotImplementedError

    def get_max_heating_system_lag(self) -> pd.Timedelta:
        raise NotImplementedError
