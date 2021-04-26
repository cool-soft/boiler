from typing import Optional

import pandas as pd


class HeatingObjDataProcessor:
    def process_heating_obj_df(self,
                               heating_obj_df: pd.DataFrame,
                               start_datetime: Optional[pd.Timestamp] = None,
                               end_datetime: Optional[pd.Timestamp] = None,
                               inplace: bool = False) -> pd.DataFrame:
        raise NotImplementedError
