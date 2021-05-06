import pandas as pd


class AbstractSyncTimedeltaLoader:

    def load_timedelta(self) -> pd.DataFrame:
        raise NotImplementedError
