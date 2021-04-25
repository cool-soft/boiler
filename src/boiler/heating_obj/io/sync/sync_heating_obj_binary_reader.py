from typing import BinaryIO

import pandas as pd


class SyncHeatingObjBinaryReader:

    def read_heating_obj_from_binary_io(self,
                                        binary_io: BinaryIO) -> pd.DataFrame:
        raise NotImplementedError
