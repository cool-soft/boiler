import asyncio
import logging
from typing import Optional

import pandas as pd


class AbstractSyncHeatingObjProcessor:

    def process_heating_obj(self, heating_obj: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError


class AbstractAsyncHeatingObjProcessor:

    async def process_heating_obj(self, heating_obj: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError


class AsyncAdapterForSyncHeatingObjProcessor(AbstractAsyncHeatingObjProcessor):

    def __init__(self,
                 processor: Optional[AbstractSyncHeatingObjProcessor] = None) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._processor = processor
        self._logger.debug(f"Processor is {processor}")

    def set_processor(self, processor: AbstractSyncHeatingObjProcessor):
        self._logger.debug(f"Processor is set to {processor}")
        self._processor = processor

    async def process_heating_obj(self, heating_obj: pd.DataFrame) -> pd.DataFrame:
        self._logger.debug(f"Processing heating obj in executor pool")
        loop = asyncio.get_running_loop()
        processed_heating_obj = await loop.run_in_executor(
            None,
            self._processor.process_heating_obj,
            heating_obj
        )
        return processed_heating_obj


