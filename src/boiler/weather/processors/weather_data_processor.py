import pandas as pd


class WeatherDataProcessor:
    def process_weather_df(self,
                           weather_df: pd.DataFrame,
                           start_datetime: pd.Timestamp = None,
                           end_datetime: pd.Timestamp = None,
                           inplace: bool = False) -> pd.DataFrame:
        raise NotImplementedError
