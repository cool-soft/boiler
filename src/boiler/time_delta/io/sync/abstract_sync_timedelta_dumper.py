import pandas as pd


class AbstractSyncTimedeltaDumper:

    def dump_timedelta(self, timedelta_df: pd.DataFrame) -> None:
        raise NotImplementedError
