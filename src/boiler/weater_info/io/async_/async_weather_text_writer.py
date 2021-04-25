from typing import TextIO

import pandas as pd


class AsyncWeatherTextWriter:

    async def write_weather_to_text_io(self, text_io: TextIO, weather_df: pd.DataFrame) -> None:
        raise NotImplementedError
