import logging
from typing import Union, IO

import pandas as pd

from .temp_graph_parser import TempGraphParser


class TempGraphCSVParser(TempGraphParser):

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

    def parse_temp_graph(self, data: Union[str, IO]) -> pd.DataFrame:
        self._logger.debug("Parsing temp graph")
        temp_graph = pd.read_csv(data)
        return temp_graph
