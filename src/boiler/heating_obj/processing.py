from typing import Union

import pandas as pd


class AbstractHeatingObjProcessor:

    def set_min_required_timestamp(self, timestamp: Union[pd.Timestamp, None]) -> None:
        raise NotImplementedError

    def set_max_required_timestamp(self, timestamp: Union[pd.Timestamp, None]) -> None:
        raise NotImplementedError

    def process_heating_obj(self, heating_obj: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError
