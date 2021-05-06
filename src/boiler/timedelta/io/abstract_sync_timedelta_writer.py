from typing import BinaryIO

import pandas as pd


class AbstractSyncTimedeltaWriter:

    def write_timedelta_to_binary_stream(self,
                                         binary_stream: BinaryIO,
                                         temp_graph_df: pd.DataFrame) -> None:
        raise NotImplementedError
