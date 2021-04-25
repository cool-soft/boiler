import logging

import pandas as pd

from boiler.constants import column_names
from .sync_temp_graph_dumper import SyncTempGraphDumper
from .sync_temp_graph_loader import SyncTempGraphLoader


class SyncTempGraphInMemoryDumperLoader(SyncTempGraphDumper, SyncTempGraphLoader):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        # TODO: вынести создание пустого DataFrame с заданными колонками куда-нибудь
        self._storage = pd.DataFrame(
            columns=(column_names.WEATHER_TEMP,
                     column_names.FORWARD_PIPE_COOLANT_TEMP,
                     column_names.BACKWARD_PIPE_COOLANT_TEMP)
        )

    def load_temp_graph(self) -> pd.DataFrame:
        self._logger.debug("Requested temp graph")
        temp_graph_df = self._storage.copy()
        return temp_graph_df

    def dump_temp_graph(self, temp_graph_df: pd.DataFrame) -> None:
        self._logger.debug("Storing temp graph")
        self._storage = temp_graph_df.copy()
