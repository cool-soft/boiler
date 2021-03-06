from typing import Optional

import pandas as pd


class AbstractAsyncWeatherLoader:

    async def load_weather(self,
                           start_datetime: Optional[pd.Timestamp] = None,
                           end_datetime: Optional[pd.Timestamp] = None
                           ) -> pd.DataFrame:
        raise NotImplementedError
