import pandas as pd


class AbstractAsyncTimedeltaLoader:

    async def load_timedelta(self) -> pd.DataFrame:
        raise NotImplementedError
