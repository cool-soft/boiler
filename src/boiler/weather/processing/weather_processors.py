import asyncio
import logging
from typing import Optional

import pandas as pd


class AbstractSyncWeatherProcessor:

    def process_weather_df(self, weather_df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError


class AbstractAsyncWeatherProcessor:

    async def process_weather_df(self, weather_df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError


class AsyncAdapterForSyncWeatherProcessor(AbstractAsyncWeatherProcessor):

    def __init__(self,
                 processor: Optional[AbstractSyncWeatherProcessor] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._processor = processor
        self._logger.debug(f"Processor is {processor}")

    def set_processor(self, processor: AbstractSyncWeatherProcessor):
        self._logger.debug(f"Processor is set to {processor}")
        self._processor = processor

    async def process_weather_df(self, weather_df: pd.DataFrame) -> pd.DataFrame:
        self._logger.debug(f"Processing weather_df in executor pool")
        loop = asyncio.get_running_loop()
        processed_weather_df = await loop.run_in_executor(
            None,
            self._processor.process_weather_df,
            weather_df
        )
        return processed_weather_df


