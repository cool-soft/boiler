import pandas as pd


class AbstractSyncTempGraphLoader:

    def load_temp_graph(self) -> pd.DataFrame:
        raise NotImplementedError
