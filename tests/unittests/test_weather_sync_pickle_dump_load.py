import pytest

from boiler.weather.io.sync.sync_weather_file_dumper import SyncWeatherFileDumper
from boiler.weather.io.sync.sync_weather_file_loader import SyncWeatherFileLoader
from boiler.weather.io.sync.sync_weather_pickle_reader import SyncWeatherPickleReader
from boiler.weather.io.sync.sync_weather_pickle_writer import SyncWeatherPickleWriter
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
        return SyncWeatherFileLoader(
            filepath=filepath,
            reader=reader
        )

    @pytest.fixture
    def dumper(self, writer, filepath):
        return SyncWeatherFileDumper(
            filepath=filepath,
            writer=writer
        )
