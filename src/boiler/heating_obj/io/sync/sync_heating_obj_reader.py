from typing import BinaryIO

import pandas as pd


class SyncHeatingObjReader:

    def read_heating_obj_from_binary_stream(self,
                                            binary_stream: BinaryIO) -> pd.DataFrame:
        raise NotImplementedError
