from typing import Optional

import pandas as pd


class TempRequirementsDBSyncRepository:

    def get_temp_requirements(self,
                              start_datetime: Optional[pd.Timestamp] = None,
                              end_datetime: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        raise NotImplementedError

    def set_temp_requirements(self, temp_requirements_df: pd.DataFrame) -> None:
        raise NotImplementedError

    def update_temp_requirements(self, temp_requirements_df: pd.DataFrame) -> None:
        raise NotImplementedError

    def delete_temp_requirements_older_than(self, datetime: pd.Timestamp) -> None:
        raise NotImplementedError

    def get_max_timestamp(self) -> pd.Timestamp:
        raise NotImplementedError
