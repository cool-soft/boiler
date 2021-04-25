from typing import TextIO

import pandas as pd


class AsyncWeatherTextReader:

    async def read_weather_from_text_io(self, text_io: TextIO) -> pd.DataFrame:
        raise NotImplementedError
