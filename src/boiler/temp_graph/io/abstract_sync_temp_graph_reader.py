from typing import BinaryIO

import pandas as pd


class AbstractSyncTempGraphReader:

    def read_temp_graph_from_binary_stream(self, binary_stream: BinaryIO) -> pd.DataFrame:
        raise NotImplementedError
