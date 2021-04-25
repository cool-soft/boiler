import asyncio
import logging
from concurrent.futures.thread import ThreadPoolExecutor
from typing import TextIO, Optional, Union

import pandas as pd

from .async_weather_text_reader import AsyncWeatherTextReader
from ..sync.sync_weather_text_reader import SyncWeatherTextReader


class AsyncWeatherSyncTextReaderAdapter(AsyncWeatherTextReader):

    def __init__(self,
                 sync_reader: Optional[SyncWeatherTextReader] = None,
                 executor: Optional[ThreadPoolExecutor] = None):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._sync_reader = sync_reader
        self._executor = executor

        self._logger.debug(f"Sync reader is {sync_reader}")
        self._logger.debug(f"Executor is {executor}")

    def set_sync_reader(self, reader: SyncWeatherTextReader):
        self._logger.debug(f"Sync reader is set to {reader}")
        self._sync_reader = reader

    def set_executor(self, executor: Union[ThreadPoolExecutor, None]):
        self._logger.debug(f"Executor is set to {executor}")
        self._executor = executor

    async def read_weather_from_text_io(self, text_io: TextIO) -> pd.DataFrame:
        self._logger.debug("Starting read operation in executor")
        loop = asyncio.get_running_loop()
        weather_df = await loop.run_in_executor(
            self._executor,
            self._sync_reader.read_weather_from_text_io,
            text_io
        )
        self._logger.debug("Read operation is finished")
        return weather_df
