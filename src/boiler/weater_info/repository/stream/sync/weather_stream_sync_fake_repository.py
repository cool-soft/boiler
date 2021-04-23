import logging
from typing import Optional

import pandas as pd

from boiler.constants import column_names
from .weather_stream_sync_repository import WeatherStreamSyncRepository


class WeatherStreamSyncFakeRepository(WeatherStreamSyncRepository):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        # TODO: вынести создание пустого DataFrame с заданными колонками куда-нибудь
        self._cache = pd.DataFrame(
            columns=(column_names.TIMESTAMP,
                     column_names.WEATHER_TEMP)
        )

    def get_weather_info(self,
                         start_datetime: Optional[pd.Timestamp] = None,
                         end_datetime: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        self._logger.debug("Requested weather info")
        weather_df = self._cache.copy()
        return weather_df

    def set_weather_info(self, weather_df: pd.DataFrame) -> None:
        self._logger.debug("Weather info is stored")
        self._cache = weather_df.copy()
