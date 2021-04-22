from typing import Union, IO

import pandas as pd


class TempGraphParser:

    def parse_temp_graph(self, data: Union[str, IO]) -> pd.DataFrame:
        raise NotImplementedError
