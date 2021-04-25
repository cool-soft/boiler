import pytest

from boiler.weather.io.sync.sync_weather_binary_file_dumper import SyncWeatherBinaryFileDumper
from boiler.weather.io.sync.sync_weather_binary_file_loader import SyncWeatherBinaryFileLoader
from boiler.weather.io.sync.sync_weather_pickle_writer import SyncWeatherPickleWriter
from boiler.weather.io.sync.sync_weather_pickle_reader import SyncWeatherPickleReader
from unittests.weather_sync_dump_load_testing import WeatherSyncDumpLoadTesting


class TestWeatherSyncPickleDumpLoad(WeatherSyncDumpLoadTesting):

    @pytest.fixture
    def filepath(self, tmp_path):
        return tmp_path / "weather.pickle"

    @pytest.fixture
    def writer(self):
        return SyncWeatherPickleWriter()

    @pytest.fixture
    def reader(self):
        return SyncWeatherPickleReader()

    @pytest.fixture
    def loader(self, reader, filepath):
        return SyncWeatherBinaryFileLoader(
            filepath=filepath,
            reader=reader
        )

    @pytest.fixture
    def dumper(self, writer, filepath):
        return SyncWeatherBinaryFileDumper(
            filepath=filepath,
            writer=writer
        )
