import pytest

from boiler.temp_graph.io.sync_temp_graph_csv_reader import SyncTempGraphCSVReader
from boiler.temp_graph.io.sync_temp_graph_csv_writer import SyncTempGraphCSVWriter
from boiler.temp_graph.io.sync_temp_graph_file_dumper import SyncTempGraphFileDumper
from boiler.temp_graph.io.sync_temp_graph_file_loader import SyncTempGraphFileLoader
from temp_graph_sync_dump_load_testing import TempGraphSyncDumpLoadTesting


class TestTempGraphSyncCSVDumpLoad(TempGraphSyncDumpLoadTesting):

    @pytest.fixture
    def filepath(self, tmp_path):
        return tmp_path / "temp_graph.csv"

    @pytest.fixture
    def writer(self):
        return SyncTempGraphCSVWriter()

    @pytest.fixture
    def reader(self):
        return SyncTempGraphCSVReader()

    @pytest.fixture
    def loader(self, reader, filepath):
        return SyncTempGraphFileLoader(
            filepath=filepath,
            reader=reader
        )

    @pytest.fixture
    def dumper(self, writer, filepath):
        return SyncTempGraphFileDumper(
            filepath=filepath,
            writer=writer
        )
