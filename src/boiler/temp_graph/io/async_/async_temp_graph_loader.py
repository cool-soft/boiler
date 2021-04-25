import pandas as pd


class AsyncTempGraphLoader:

    async def load_temp_graph(self) -> pd.DataFrame:
        raise NotImplementedError
