import pandas as pd


class AsyncTempGraphDumper:

    async def dump_temp_graph(self, temp_graph_df: pd.DataFrame) -> None:
        raise NotImplementedError
