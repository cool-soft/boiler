import pandas as pd


class AbstractSyncWeatherDumper:

    def dump_weather(self, weather_df: pd.DataFrame) -> None:
        raise NotImplementedError
