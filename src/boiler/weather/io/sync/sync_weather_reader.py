from typing import BinaryIO

import pandas as pd


class SyncWeatherReader:

    def read_weather_from_binary_stream(self, binary_stream: BinaryIO) -> pd.DataFrame:
        raise NotImplementedError
