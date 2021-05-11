from typing import BinaryIO

import pandas as pd


class AbstractSyncTimedeltaWriter:

    def write_timedelta_to_binary_stream(self,
                                         binary_stream: BinaryIO,
                                         timedelta_df: pd.DataFrame) -> None:
        raise NotImplementedError
