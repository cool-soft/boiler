from typing import BinaryIO

import pandas as pd


class SyncHeatingObjWriter:

    def write_heating_obj_to_binary_stream(self,
                                           binary_stream: BinaryIO,
                                           heating_obj_df: pd.DataFrame) -> None:
        raise NotImplementedError
