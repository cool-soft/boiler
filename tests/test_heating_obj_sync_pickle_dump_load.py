import pytest

from boiler.heating_obj.io.sync_heating_obj_file_dumper import SyncHeatingObjFileDumper
from boiler.heating_obj.io.sync_heating_obj_file_loader import SyncHeatingObjFileLoader
from boiler.heating_obj.io.sync_heating_obj_pickle_reader import SyncHeatingObjPickleReader
from boiler.heating_obj.io.sync_heating_obj_pickle_writer import SyncHeatingObjPickleWriter
from heating_obj_sync_dump_load_testing import HeatingObjSyncDumpLoadTesting


class TestHeatingObjSyncPickleDumpLoad(HeatingObjSyncDumpLoadTesting):

    @pytest.fixture
    def filepath(self, tmp_path):
        return tmp_path / "weather.pickle"

    @pytest.fixture
    def writer(self):
        return SyncHeatingObjPickleWriter()

    @pytest.fixture
    def reader(self):
        return SyncHeatingObjPickleReader()

    @pytest.fixture
    def loader(self, reader, filepath):
        return SyncHeatingObjFileLoader(
            filepath=filepath,
            reader=reader
        )

    @pytest.fixture
    def dumper(self, writer, filepath):
        return SyncHeatingObjFileDumper(
            filepath=filepath,
            writer=writer
        )
