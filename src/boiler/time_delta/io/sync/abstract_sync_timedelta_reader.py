from typing import BinaryIO

import pandas as pd


class AbstractSyncTimedeltaReader:

    def read_timedelta_from_binary_stream(self, binary_stream: BinaryIO) -> pd.DataFrame:
        raise NotImplementedError
