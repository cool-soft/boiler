import pandas as pd
from boiler.logger import boiler_logger

from boiler.constants import dataset_prototypes
from boiler.temp_graph.io.abstract_sync_temp_graph_dumper import AbstractSyncTempGraphDumper
from boiler.temp_graph.io.abstract_sync_temp_graph_loader import AbstractSyncTempGraphLoader


class SyncTempGraphInMemoryDumperLoader(AbstractSyncTempGraphDumper, AbstractSyncTempGraphLoader):

    def __init__(self) -> None:
        self._storage = dataset_prototypes.TEMP_GRAPH.copy()

        boiler_logger.debug("Creating instance")

    def load_temp_graph(self) -> pd.DataFrame:
        boiler_logger.debug("Requested temp graph")
        temp_graph_df = self._storage.copy()
        return temp_graph_df

    def dump_temp_graph(self,
                        temp_graph_df: pd.DataFrame
                        ) -> None:
        boiler_logger.debug("Storing temp graph")
        self._storage = temp_graph_df.copy()
