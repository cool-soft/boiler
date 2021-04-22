import pandas as pd


class HeatingSystemDataInterpolator:
    def interpolate_data(self,
                         boiler_df: pd.DataFrame,
                         start_datetime: pd.Timestamp = None,
                         end_datetime: pd.Timestamp = None,
                         inplace: bool = False) -> pd.DataFrame:
        raise NotImplementedError
