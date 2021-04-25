import pytest

from boiler.heating_obj.io.sync.sync_heating_obj_binary_file_dumper import SyncHeatingObjBinaryFileDumper
from boiler.heating_obj.io.sync.sync_heating_obj_binary_file_loader import SyncHeatingObjBinaryFileLoader
from boiler.heating_obj.io.sync.sync_heating_obj_pickle_reader import SyncHeatingObjPickleReader
from boiler.heating_obj.io.sync.sync_heating_obj_pickle_writer import SyncHeatingObjPickleWriter
from unittests.heating_obj_sync_dump_load_testing import HeatingObjSyncDumpLoadTesting


class TestHeatingObjSyncPickleDumpLoad(HeatingObjSyncDumpLoadTesting):

    @pytest.fixture
    def filepath(self, tmp_path):
        return tmp_path / "weather.csv"

    @pytest.fixture
    def writer(self):
        return SyncHeatingObjPickleWriter()

    @pytest.fixture
    def reader(self):
        return SyncHeatingObjPickleReader()

    @pytest.fixture
    def loader(self, reader, filepath):
        return SyncHeatingObjBinaryFileLoader(
            filepath=filepath,
            reader=reader
        )

    @pytest.fixture
    def dumper(self, writer, filepath):
        return SyncHeatingObjBinaryFileDumper(
            filepath=filepath,
            writer=writer
        )
