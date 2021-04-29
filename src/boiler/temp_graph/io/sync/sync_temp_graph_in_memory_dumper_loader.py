import logging

import pandas as pd

from boiler.constants import dataset_prototypes
from boiler.temp_graph.io.sync.sync_temp_graph_dumper import SyncTempGraphDumper
from boiler.temp_graph.io.sync.sync_temp_graph_loader import SyncTempGraphLoader


class SyncTempGraphInMemoryDumperLoader(SyncTempGraphDumper, SyncTempGraphLoader):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._storage = dataset_prototypes.TEMP_GRAPH.copy()

    def load_temp_graph(self) -> pd.DataFrame:
        self._logger.debug("Requested temp graph")
        temp_graph_df = self._storage.copy()
        return temp_graph_df

    def dump_temp_graph(self, temp_graph_df: pd.DataFrame) -> None:
        self._logger.debug("Storing temp graph")
        self._storage = temp_graph_df.copy()
