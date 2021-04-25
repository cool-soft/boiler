import pytest

from boiler.temp_graph.io.sync.sync_temp_graph_csv_reader import SyncTempGraphCSVReader
from boiler.temp_graph.io.sync.sync_temp_graph_csv_writer import SyncTempGraphCSVWriter
from boiler.temp_graph.io.sync.sync_temp_graph_text_file_dumper import SyncTempGraphTextFileDumper
from boiler.temp_graph.io.sync.sync_temp_graph_text_file_loader import SyncTempGraphTextFileLoader
from unittests.temp_graph_sync_dump_load_testing import TempGraphSyncDumpLoadTesting


class TestWeatherSyncCSVDumpLoad(TempGraphSyncDumpLoadTesting):

    @pytest.fixture
    def filepath(self, tmp_path):
        return tmp_path / "weather.csv"

    @pytest.fixture
    def writer(self):
        return SyncTempGraphCSVWriter()

    @pytest.fixture
    def reader(self):
        return SyncTempGraphCSVReader()

    @pytest.fixture
    def loader(self, reader, filepath):
        return SyncTempGraphTextFileLoader(
            filepath=filepath,
            reader=reader
        )

    @pytest.fixture
    def dumper(self, writer, filepath):
        return SyncTempGraphTextFileDumper(
            filepath=filepath,
            writer=writer
        )
