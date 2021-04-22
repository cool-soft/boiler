from typing import Optional

import pandas as pd


class WeatherDataInterpolator:
    def interpolate_weather_data(self,
                                 weather_data: pd.DataFrame,
                                 start_datetime: Optional[pd.Timestamp]= None,
                                 end_datetime: Optional[pd.Timestamp] = None,
                                 inplace: bool = False) -> pd.DataFrame:
        raise NotImplementedError
