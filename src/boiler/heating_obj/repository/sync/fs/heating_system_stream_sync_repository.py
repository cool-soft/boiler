from typing import List, Optional

import pandas as pd


class HeatingSystemStreamSyncRepository:

    def list(self) -> List[str]:
        raise NotImplementedError

    def get_dataset(self,
                    dataset_id: str,
                    start_datetime: Optional[pd.Timestamp] = None,
                    end_datetime: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        raise NotImplementedError

    def set_dataset(self, dataset_id: str, dataset: pd.DataFrame) -> None:
        raise NotImplementedError
