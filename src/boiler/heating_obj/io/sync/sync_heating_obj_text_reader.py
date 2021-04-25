from typing import TextIO

import pandas as pd


class SyncHeatingObjTextReader:

    def read_heating_obj_from_text_io(self,
                                      text_io: TextIO) -> pd.DataFrame:
        raise NotImplementedError
