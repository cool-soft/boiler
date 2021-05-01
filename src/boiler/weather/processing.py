from typing import Union

import pandas as pd


class AbstractWeatherProcessor:

    def process_weather_df(self,
                           weather_df: pd.DataFrame,
                           min_required_timestamp: Union[pd.Timestamp, None],
                           max_required_timestamp: Union[pd.Timestamp, None]
                           ) -> pd.DataFrame:
        raise NotImplementedError
