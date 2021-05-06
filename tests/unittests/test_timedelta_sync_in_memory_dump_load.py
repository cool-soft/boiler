import pytest

from boiler.timedelta.io.sync_timedelta_in_memory_dumper_loader import SyncTimedeltaInMemoryDumperLoader
from unittests.timedelta_sync_dump_load_testing import TimedeltaSyncDumpLoadTesting


class TestTimedeltaInMemoryDumpLoad(TimedeltaSyncDumpLoadTesting):

    @pytest.fixture
    def in_memory_dumper_loader(self):
        return SyncTimedeltaInMemoryDumperLoader()

    @pytest.fixture
    def loader(self, in_memory_dumper_loader):
        return in_memory_dumper_loader

    @pytest.fixture
    def dumper(self, in_memory_dumper_loader):
        return in_memory_dumper_loader
