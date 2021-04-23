import pandas as pd


class TempGraphStreamAsyncRepository:

    async def get_temp_graph(self) -> pd.DataFrame:
        raise NotImplementedError

    async def set_temp_graph(self, temp_graph: pd.DataFrame) -> None:
        raise NotImplementedError
