from typing import TextIO

import pandas as pd


class SyncTempGraphTextReader:

    def read_temp_graph_from_text_io(self, text_io: TextIO) -> pd.DataFrame:
        raise NotImplementedError
