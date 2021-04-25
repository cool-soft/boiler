from typing import BinaryIO

import pandas as pd


class SyncTempGraphBinaryWriter:

    def write_temp_graph_to_binary_io(self,
                                      binary_io: BinaryIO,
                                      weather_df: pd.DataFrame) -> None:
        raise NotImplementedError
