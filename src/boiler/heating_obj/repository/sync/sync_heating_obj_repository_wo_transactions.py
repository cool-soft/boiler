from typing import List, Optional

import pandas as pd


class SyncHeatingObjRepositoryWithoutTransactions:

    def list(self) -> List[str]:
        raise NotImplementedError

    def get_dataset(self,
                    dataset_id: str,
                    start_datetime: Optional[pd.Timestamp] = None,
                    end_datetime: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        raise NotImplementedError

    def set_dataset(self, dataset_id: str, heating_obj_df: pd.DataFrame) -> None:
        raise NotImplementedError

    def del_dataset(self, dataset_id: str) -> None:
        raise NotImplementedError
