import pytest

from boiler.heating_obj.io.sync.sync_heating_obj_file_dumper import SyncHeatingObjFileDumper
from boiler.heating_obj.io.sync.sync_heating_obj_file_loader import SyncHeatingObjFileLoader
from boiler.heating_obj.io.sync.sync_heating_obj_pickle_reader import SyncHeatingObjPickleReader
from boiler.heating_obj.io.sync.sync_heating_obj_pickle_writer import SyncHeatingObjPickleWriter
from unittests.heating_obj_sync_dump_load_testing import HeatingObjSyncDumpLoadTesting


class TestHeatingObjSyncPickleDumpLoad(HeatingObjSyncDumpLoadTesting):

    @pytest.fixture
    def filepath(self, tmp_path):
        return tmp_path / "heating_obj.pickle"

    @pytest.fixture
    def writer(self):
        return SyncHeatingObjPickleWriter()

    @pytest.fixture
    def reader(self):
        return SyncHeatingObjPickleReader()

    @pytest.fixture
    def loader(self, reader, filepath, filter_algorithm):
        return SyncHeatingObjFileLoader(
            filepath=filepath,
            reader=reader,
            filter_algorithm=filter_algorithm
        )

    @pytest.fixture
    def dumper(self, writer, filepath):
        return SyncHeatingObjFileDumper(
            filepath=filepath,
            writer=writer
        )
