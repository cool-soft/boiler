import pandas as pd
import pytest

from boiler.constants import column_names
from boiler.temp_graph.repository.stream.sync.temp_graph_stream_sync_pickle_repository import \
    TempGraphStreamSyncPickleRepository
from unittests.temp_graph_stream_sync_repository_base_operations_testing \
    import TempGraphStreamSyncRepositoryBaseOperationsTesting


class TestTempGraphStreamSyncPickleRepository(TempGraphStreamSyncRepositoryBaseOperationsTesting):

    @pytest.fixture
    def filepath(self, tmp_path):
        return tmp_path / "temp_graph.pickle"

    @pytest.fixture
    def repository(self, filepath):
        repo = TempGraphStreamSyncPickleRepository(filepath)
        return repo

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
