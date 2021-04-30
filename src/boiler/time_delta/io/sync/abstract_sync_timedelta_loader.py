import pandas as pd


class AbstractSyncTimedeltaLoader:

    async def load_timedelta(self) -> pd.DataFrame:
        raise NotImplementedError
