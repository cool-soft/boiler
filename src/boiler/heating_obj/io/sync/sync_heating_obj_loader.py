from typing import Optional

import pandas as pd


class SyncHeatingObjLoader:

    def load_heating_obj(self,
                         start_datetime: Optional[pd.Timestamp] = None,
                         end_datetime: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        raise NotImplementedError
