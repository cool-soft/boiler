import pytest

from boiler.temp_graph.io.sync.sync_temp_graph_binary_file_dumper import SyncTempGraphBinaryFileDumper
from boiler.temp_graph.io.sync.sync_temp_graph_binary_file_loader import SyncTempGraphBinaryFileLoader
from boiler.temp_graph.io.sync.sync_temp_graph_pickle_reader import SyncTempGraphPickleReader
from boiler.temp_graph.io.sync.sync_temp_graph_pickle_writer import SyncTempGraphPickleWriter
from unittests.temp_graph_sync_dump_load_testing import TempGraphSyncDumpLoadTesting


class TestWeatherSyncPickleDumpLoad(TempGraphSyncDumpLoadTesting):

    @pytest.fixture
    def filepath(self, tmp_path):
        return tmp_path / "weather.csv"

    @pytest.fixture
    def writer(self):
        return SyncTempGraphPickleWriter()

    @pytest.fixture
    def reader(self):
        return SyncTempGraphPickleReader()

    @pytest.fixture
    def loader(self, reader, filepath):
        return SyncTempGraphBinaryFileLoader(
            filepath=filepath,
            reader=reader
        )

    @pytest.fixture
    def dumper(self, writer, filepath):
        return SyncTempGraphBinaryFileDumper(
            filepath=filepath,
            writer=writer
        )
