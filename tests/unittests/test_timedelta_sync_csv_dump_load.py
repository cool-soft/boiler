import pytest

from boiler.time_delta.io.sync_timedelta_csv_reader import SyncTimedeltaCSVReader
from boiler.time_delta.io.sync_timedelta_csv_writer import SyncTimedeltaCSVWriter
from boiler.time_delta.io.sync_timedelta_file_dumper import SyncTimedeltaFileDumper
from boiler.time_delta.io.sync_timedelta_file_loader import SyncTimedeltaFileLoader
from unittests.timedelta_sync_dump_load_testing import TimedeltaSyncDumpLoadTesting


class TestTimedeltaSyncCSVDumpLoad(TimedeltaSyncDumpLoadTesting):

    @pytest.fixture
    def filepath(self, tmp_path):
        return tmp_path / "timedelta.csv"

    @pytest.fixture
    def writer(self):
        return SyncTimedeltaCSVWriter()

    @pytest.fixture
    def reader(self):
        return SyncTimedeltaCSVReader()

    @pytest.fixture
    def loader(self, reader, filepath):
        return SyncTimedeltaFileLoader(
            filepath=filepath,
            reader=reader
        )

    @pytest.fixture
    def dumper(self, writer, filepath):
        return SyncTimedeltaFileDumper(
            filepath=filepath,
            writer=writer
        )
