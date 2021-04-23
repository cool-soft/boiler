import logging
import os

import pandas as pd

from .temp_graph_stream_sync_repository import TempGraphStreamSyncRepository


class TempGraphStreamSyncPickleRepository(TempGraphStreamSyncRepository):

    def __init__(self,
                 filepath: str = "./storage/temp_graph.pickle") -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance of provider")

        self._filepath = filepath

        self._logger.debug(f"Filepath is {filepath}")

    def set_filepath(self, filepath: str) -> None:
        self._logger.debug(f"Filepath is set to {filepath}")
        self._filepath = filepath

    def get_temp_graph(self) -> pd.DataFrame:
        filepath = os.path.abspath(self._filepath)
        self._logger.debug(f"Loading temp graph from {filepath}")
        temp_graph = pd.read_pickle(filepath)
        self._logger.debug("temp graph is loaded")
        return temp_graph

    def set_temp_graph(self, temp_graph: pd.DataFrame) -> None:
        filepath = os.path.abspath(self._filepath)
        self._logger.debug(f"Storing temp graph to {filepath}")
        temp_graph.to_pickle(filepath)
        self._logger.debug("temp graph is stored")

