import pytest

from boiler.heating_obj.io.sync.sync_heating_obj_csv_reader import SyncHeatingObjCSVReader
from boiler.heating_obj.io.sync.sync_heating_obj_csv_writer import SyncHeatingObjCSVWriter
from boiler.heating_obj.io.sync.sync_heating_obj_text_file_dumper import SyncHeatingObjTextFileDumper
from boiler.heating_obj.io.sync.sync_heating_obj_text_file_loader import SyncHeatingObjTextFileLoader
from unittests.heating_obj_sync_dump_load_testing import HeatingObjSyncDumpLoadTesting


class TestHeatingObjSyncCSVDumpLoad(HeatingObjSyncDumpLoadTesting):

    @pytest.fixture
    def filepath(self, tmp_path):
        return tmp_path / "weather.csv"

    @pytest.fixture
    def writer(self):
        return SyncHeatingObjCSVWriter()

    @pytest.fixture
    def reader(self):
        return SyncHeatingObjCSVReader()

    @pytest.fixture
    def loader(self, reader, filepath):
        return SyncHeatingObjTextFileLoader(
            filepath=filepath,
            reader=reader
        )

    @pytest.fixture
    def dumper(self, writer, filepath):
        return SyncHeatingObjTextFileDumper(
            filepath=filepath,
            writer=writer
        )
