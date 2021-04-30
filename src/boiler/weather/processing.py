from typing import Union

import pandas as pd


class AbstractWeatherProcessor:

    def set_min_required_timestamp(self, timestamp: Union[pd.Timestamp, None]) -> None:
        raise NotImplementedError

    def set_max_required_timestamp(self, timestamp: Union[pd.Timestamp, None]) -> None:
        raise NotImplementedError

    def process_weather_df(self, weather_df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError
