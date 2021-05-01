import pandas as pd


class AbstractAsyncTempGraphLoader:

    async def load_temp_graph(self) -> pd.DataFrame:
        raise NotImplementedError
