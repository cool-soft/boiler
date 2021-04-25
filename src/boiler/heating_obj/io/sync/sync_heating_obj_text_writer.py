from typing import TextIO

import pandas as pd


class SyncHeatingObjTextWriter:

    def write_heating_obj_to_text_io(self,
                                     text_io: TextIO,
                                     heating_obj_df: pd.DataFrame) -> None:
        raise NotImplementedError
