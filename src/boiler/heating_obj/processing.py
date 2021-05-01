from typing import Union

import pandas as pd


class AbstractHeatingObjProcessor:

    def process_heating_obj(self,
                            heating_obj_df: pd.DataFrame,
                            min_required_timestamp: Union[pd.Timestamp, None],
                            max_required_timestamp: Union[pd.Timestamp, None]) -> pd.DataFrame:
        raise NotImplementedError
