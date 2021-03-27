import pandas as pd


class WeatherRepository:

    def get_weather_info(self,
                         start_datetime: pd.Timestamp = None,
                         end_datetime: pd.Timestamp = None) -> pd.DataFrame:
        raise NotImplementedError

    def set_weather_info(self, weather_df: pd.DataFrame):
        raise NotImplementedError

    def update_weather_info(self, weather_df: pd.DataFrame):
        raise NotImplementedError

    def delete_weather_info(self,
                            start_datetime: pd.Timestamp = None,
                            end_datetime: pd.Timestamp = None):
        raise NotImplementedError
