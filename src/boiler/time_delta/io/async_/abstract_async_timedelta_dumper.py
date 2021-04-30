import pandas as pd


class AbstractAsyncTimedeltaDumper:

    async def dump_timedelta(self, timedelta_df: pd.DataFrame) -> None:
        raise NotImplementedError
