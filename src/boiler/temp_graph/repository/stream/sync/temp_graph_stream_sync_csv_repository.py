import logging
import os
from typing import Optional

import pandas as pd

from boiler.temp_graph.io.temp_graph_parser import TempGraphParser
from .temp_graph_stream_sync_repository import TempGraphStreamSyncRepository


class TempGraphStreamSyncCSVRepository(TempGraphStreamSyncRepository):

    def __init__(self,
                 filepath: str = "./storage/temp_graph.csv",
                 parser: Optional[TempGraphParser] = None,
                 encoding="utf-8") -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._filepath = filepath
        self._parser = parser
        self._encoding = encoding

        self._logger.debug(f"Filepath is {filepath}")
        self._logger.debug(f"Parse is {parser}")
        self._logger.debug(f"Encoding is {encoding}")

    def set_filepath(self, filepath: str) -> None:
        self._logger.debug(f"Filepath is set to {filepath}")
        self._filepath = filepath

    def set_parser(self, parser: TempGraphParser) -> None:
        self._logger.debug(f"Parser is set to {parser}")
        self._parser = parser

    def set_encoding(self, encoding: str) -> None:
        self._logger.debug(f"Encoding is set to {encoding}")
        self._encoding = encoding

    def get_temp_graph(self) -> pd.DataFrame:
        filepath = os.path.abspath(self._filepath)
        self._logger.debug(f"Loading temp graph from {filepath}")
        with open(filepath, mode="r", encoding=self._encoding) as input_file:
            temp_graph = self._parser.parse_temp_graph(input_file)
        self._logger.debug("temp graph is loaded")
        return temp_graph

    def set_temp_graph(self, temp_graph: pd.DataFrame) -> None:
        filepath = os.path.abspath(self._filepath)
        self._logger.debug(f"Storing temp graph to {filepath}")
        with open(filepath, mode="w", encoding=self._encoding) as output_file:
            temp_graph.to_csv(output_file, index=False)
        self._logger.debug("temp graph is stored")
