import logging

import pandas as pd

from boiler.constants import column_names
from .temp_graph_stream_async_repository import TempGraphStreamAsyncRepository


class TempGraphStreamSyncFakeRepository(TempGraphStreamAsyncRepository):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance of provider")

        # TODO: вынести создание пустого DataFrame с заданными колонками куда-нибудь
        self._cache = pd.DataFrame(
            columns=(column_names.TIMESTAMP,
                     column_names.FORWARD_PIPE_COOLANT_TEMP,
                     column_names.BACKWARD_PIPE_COOLANT_TEMP)
        )

    async def get_temp_graph(self) -> pd.DataFrame:
        self._logger.debug("Requested temp graph")
        temp_graph_df = self._cache.copy()
        return temp_graph_df

    async def set_temp_graph(self, temp_graph: pd.DataFrame) -> None:
        self._logger.debug("Temp graph is stored")
        self._cache = temp_graph.copy()