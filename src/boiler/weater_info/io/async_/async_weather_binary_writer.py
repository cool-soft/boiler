from typing import BinaryIO

import pandas as pd


class AsyncWeatherBinaryWriter:

    async def write_weather_to_binary_io(self, binary_io: BinaryIO, weather_df: pd.DataFrame) -> None:
        raise NotImplementedError
