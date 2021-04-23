import pandas as pd


class TempGraphStreamSyncRepository:

    def get_temp_graph(self) -> pd.DataFrame:
        raise NotImplementedError

    def set_temp_graph(self, temp_graph: pd.DataFrame) -> None:
        raise NotImplementedError
