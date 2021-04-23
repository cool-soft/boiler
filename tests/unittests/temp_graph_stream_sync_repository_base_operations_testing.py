import pandas as pd
import pytest

from boiler.constants import column_names


# noinspection PyMethodMayBeStatic
class TempGraphStreamSyncRepositoryBaseOperationsTesting:

    @pytest.fixture
    def temp_graph(self):
        temp_graph = pd.DataFrame(
            columns=(column_names.WEATHER_TEMP,
                     column_names.FORWARD_PIPE_COOLANT_TEMP,
                     column_names.BACKWARD_PIPE_COOLANT_TEMP)
        )

        temp_graph = temp_graph.append([
            {column_names.WEATHER_TEMP: -10,
             column_names.FORWARD_PIPE_COOLANT_TEMP: 50.1,
             column_names.BACKWARD_PIPE_COOLANT_TEMP: 45},
            {column_names.WEATHER_TEMP: 0,
             column_names.FORWARD_PIPE_COOLANT_TEMP: 40,
             column_names.BACKWARD_PIPE_COOLANT_TEMP: 38.7},
            {column_names.WEATHER_TEMP: 10,
             column_names.FORWARD_PIPE_COOLANT_TEMP: 30,
             column_names.BACKWARD_PIPE_COOLANT_TEMP: 28},
        ])

        return temp_graph

    def test_temp_graph_repository_set_get(self, temp_graph, repository):
        repository.set_temp_graph(temp_graph)
        loaded_temp_graph = repository.get_temp_graph()
        assert loaded_temp_graph.to_dict("records") == temp_graph.to_dict("records")
