import logging
from typing import Optional

import pandas as pd

from boiler.constants import column_names, dataset_prototypes
from boiler.temp_requirements.repository.db.async_.temp_requirements_db_async_repository \
    import TempRequirementsDBAsyncRepository


class TempRequirementsDBAsyncFakeRepository(TempRequirementsDBAsyncRepository):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance of provider")

        self._cache = dataset_prototypes.TEMP_REQUIREMENTS.copy()

    async def get_temp_requirements(self,
                                    start_datetime: Optional[pd.Timestamp] = None,
                                    end_datetime: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        self._logger.debug(f"Requested temp requirements from {start_datetime} to {end_datetime}")

        control_df = self._cache.copy()
        if start_datetime is not None:
            control_df = control_df[control_df[column_names.TIMESTAMP] >= start_datetime]
        if end_datetime is not None:
            control_df = control_df[control_df[column_names.TIMESTAMP] <= end_datetime]

        return control_df

    async def set_temp_requirements(self, temp_requirements_df: pd.DataFrame) -> None:
        self._logger.debug("Temp requirements are stored")

        self._cache = temp_requirements_df.copy()

    async def update_temp_requirements(self, temp_requirements_df: pd.DataFrame) -> None:
        self._logger.debug("Stored temp requirements are updated")

        cache_df = self._cache.copy()
        cache_df = cache_df.append(temp_requirements_df)
        cache_df = cache_df.drop_duplicates(column_names.TIMESTAMP, keep='last')
        cache_df = cache_df.sort_values(column_names.TIMESTAMP, ignore_index=True)
        self._cache = cache_df

    async def delete_temp_requirements_older_than(self, datetime: pd.Timestamp) -> None:
        self._logger.debug(f"Requested deleting temp requirements older than {datetime}")

        self._cache = self._cache[self._cache[column_names.TIMESTAMP] >= datetime].copy()

    async def get_max_timestamp(self) -> pd.Timestamp:
        self._logger.debug("Requested max timestamp")

        max_timestamp = None
        if not self._cache.empty:
            max_timestamp = self._cache[column_names.TIMESTAMP].max()
        self._logger.debug(f"Max timestamp is {max_timestamp}")

        return max_timestamp
