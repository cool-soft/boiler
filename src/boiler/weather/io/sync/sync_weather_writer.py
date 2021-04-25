from typing import BinaryIO

import pandas as pd


class SyncWeatherWriter:

    def write_weather_to_binary_stream(self, binary_stream: BinaryIO, weather_df: pd.DataFrame) -> None:
        raise NotImplementedError
