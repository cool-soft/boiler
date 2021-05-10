import pytest

from boiler.heating_obj.io.sync_heating_obj_in_memory_dumper_loader import SyncHeatingObjInMemoryDumperLoader
from heating_obj_sync_dump_load_testing import HeatingObjSyncDumpLoadTesting


class TestHeatingObjSyncInMemoryDumpLoad(HeatingObjSyncDumpLoadTesting):

    @pytest.fixture
    def dumper_loader(self):
        return SyncHeatingObjInMemoryDumperLoader()

    @pytest.fixture
    def loader(self, dumper_loader):
        return dumper_loader

    @pytest.fixture
    def dumper(self, dumper_loader):
        return dumper_loader
