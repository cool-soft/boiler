import pytest
import pandas as pd

from boiler.constants import column_names, dataset_prototypes


# noinspection PyMethodMayBeStatic
class TempGraphSyncDumpLoadTesting:

    @pytest.fixture
    def temp_graph(self):
        temp_graph = dataset_prototypes.TEMP_GRAPH.copy()

        temp_graph = pd.concat(
            [
                temp_graph, 
                pd.DataFrame([
                    {column_names.WEATHER_TEMP: -10,
                     column_names.FORWARD_TEMP: 50.1,
                     column_names.BACKWARD_TEMP: 45},
                    {column_names.WEATHER_TEMP: 0,
                     column_names.FORWARD_TEMP: 40,
                     column_names.BACKWARD_TEMP: 38.7},
                    {column_names.WEATHER_TEMP: 10,
                     column_names.FORWARD_TEMP: 30,
                     column_names.BACKWARD_TEMP: 28},
                ])
            ]
        )

        return temp_graph

    def test_temp_graph_sync_dump_load(self, temp_graph, dumper, loader):
        dumper.dump_temp_graph(temp_graph)
        loaded_temp_graph = loader.load_temp_graph()
        assert loaded_temp_graph.to_dict("records") == temp_graph.to_dict("records")
