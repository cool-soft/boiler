import pytest

from boiler.heating_obj.io.sync.sync_heating_obj_csv_reader import SyncHeatingObjCSVReader
from boiler.heating_obj.io.sync.sync_heating_obj_csv_writer import SyncHeatingObjCSVWriter
from boiler.heating_obj.io.sync.sync_heating_obj_file_dumper import SyncHeatingObjFileDumper
from boiler.heating_obj.io.sync.sync_heating_obj_file_loader import SyncHeatingObjFileLoader
from unittests.heating_obj_sync_dump_load_testing import HeatingObjSyncDumpLoadTesting


class TestHeatingObjSyncCSVDumpLoad(HeatingObjSyncDumpLoadTesting):

    @pytest.fixture
    def filepath(self, tmp_path):
        return tmp_path / "heating_obj.csv"

    @pytest.fixture
    def writer(self):
        return SyncHeatingObjCSVWriter()

    @pytest.fixture
    def reader(self):
        return SyncHeatingObjCSVReader()

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
