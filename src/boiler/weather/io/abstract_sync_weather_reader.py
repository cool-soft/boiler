from typing import BinaryIO

import pandas as pd


class AbstractSyncWeatherReader:

    def read_weather_from_binary_stream(self, binary_stream: BinaryIO) -> pd.DataFrame:
        raise NotImplementedError
