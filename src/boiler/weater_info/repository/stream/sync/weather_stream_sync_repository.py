from typing import Optional

import pandas as pd


class WeatherStreamSyncRepository:

    def get_weather_info(self,
                         start_datetime: Optional[pd.Timestamp] = None,
                         end_datetime: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        raise NotImplementedError

    def set_weather_info(self, weather_df: pd.DataFrame) -> None:
        raise NotImplementedError
