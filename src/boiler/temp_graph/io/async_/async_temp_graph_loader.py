from typing import Optional

import pandas as pd


class AsyncTempGraphLoader:

    async def load_temp_graph(self,
                              start_datetime: Optional[pd.Timestamp] = None,
                              end_datetime: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        raise NotImplementedError
