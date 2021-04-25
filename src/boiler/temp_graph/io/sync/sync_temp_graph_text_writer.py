from typing import TextIO

import pandas as pd


class SyncTempGraphTextWriter:

    def write_temp_graph_to_text_io(self,
                                    text_io: TextIO,
                                    temp_graph_df: pd.DataFrame) -> None:
        raise NotImplementedError
