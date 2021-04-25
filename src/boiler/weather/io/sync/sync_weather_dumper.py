import pandas as pd


class SyncWeatherDumper:

    def dump_weather(self, weather_df: pd.DataFrame) -> None:
        raise NotImplementedError
