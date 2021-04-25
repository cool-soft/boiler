import pandas as pd


class AsyncWeatherDumper:

    async def dump_weather(self, weather_df: pd.DataFrame) -> None:
        raise NotImplementedError
