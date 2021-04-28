import pandas as pd


class AbstractDatasetProcessor:

    def process_df(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError
