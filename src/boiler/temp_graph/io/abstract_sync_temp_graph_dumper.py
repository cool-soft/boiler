import pandas as pd


class AbstractSyncTempGraphDumper:

    def dump_temp_graph(self, temp_graph_df: pd.DataFrame) -> None:
        raise NotImplementedError
