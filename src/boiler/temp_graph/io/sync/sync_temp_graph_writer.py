from typing import BinaryIO

import pandas as pd


class SyncTempGraphWriter:

    def write_temp_graph_to_binary_stream(self,
                                          binary_stream: BinaryIO,
                                          temp_graph_df: pd.DataFrame) -> None:
        raise NotImplementedError
