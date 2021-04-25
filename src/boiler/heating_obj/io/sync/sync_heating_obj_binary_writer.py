from typing import BinaryIO

import pandas as pd


class SyncHeatingObjBinaryWriter:

    def write_heating_obj_to_binary_io(self,
                                       binary_io: BinaryIO,
                                       heating_obj_df: pd.DataFrame) -> None:
        raise NotImplementedError
