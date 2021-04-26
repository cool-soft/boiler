import pandas as pd


class HeatingObjDataProcessor:
    def process_heating_obj_df(self,
                               boiler_df: pd.DataFrame,
                               start_datetime: pd.Timestamp = None,
                               end_datetime: pd.Timestamp = None,
                               inplace: bool = False) -> pd.DataFrame:
        raise NotImplementedError
