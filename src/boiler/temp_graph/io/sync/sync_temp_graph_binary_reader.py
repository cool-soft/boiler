from typing import BinaryIO

import pandas as pd


class SyncTempGraphBinaryReader:

    def read_temp_graph_from_binary_io(self, binary_io: BinaryIO) -> pd.DataFrame:
        raise NotImplementedError
