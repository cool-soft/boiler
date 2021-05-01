import pytest

from boiler.temp_graph.io.sync_temp_graph_in_memory_dumper_loader import SyncTempGraphInMemoryDumperLoader
from unittests.temp_graph_sync_dump_load_testing import TempGraphSyncDumpLoadTesting


class TestTempGraphInMemoryDumpLoad(TempGraphSyncDumpLoadTesting):

    @pytest.fixture
    def in_memory_dumper_loader(self):
        return SyncTempGraphInMemoryDumperLoader()

    @pytest.fixture
    def loader(self, in_memory_dumper_loader):
        return in_memory_dumper_loader

    @pytest.fixture
    def dumper(self, in_memory_dumper_loader):
        return in_memory_dumper_loader
