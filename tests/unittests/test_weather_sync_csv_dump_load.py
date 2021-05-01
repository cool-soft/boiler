import pytest

from boiler.weather.io.sync_weather_csv_reader import SyncWeatherCSVReader
from boiler.weather.io.sync_weather_csv_writer import SyncWeatherCSVWriter
from boiler.weather.io.sync_weather_file_loader import SyncWeatherFileLoader
from boiler.weather.io.sync_weather_file_dumper import SyncWeatherFileDumper
from unittests.weather_sync_dump_load_testing import WeatherSyncDumpLoadTesting


class TestWeatherSyncCSVDumpLoad(WeatherSyncDumpLoadTesting):

    @pytest.fixture
    def filepath(self, tmp_path):
        return tmp_path / "weather.csv"

    @pytest.fixture
    def writer(self):
        return SyncWeatherCSVWriter()

    @pytest.fixture
    def reader(self):
        return SyncWeatherCSVReader()

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
