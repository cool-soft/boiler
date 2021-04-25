from typing import BinaryIO

import pandas as pd


class AsyncWeatherBinaryReader:

    async def read_weather_from_binary_io(self, binary_io: BinaryIO) -> pd.DataFrame:
        raise NotImplementedError
