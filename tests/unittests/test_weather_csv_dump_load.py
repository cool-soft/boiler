import pytest

from boiler.weater_info.io.sync.sync_weather_csv_reader import SyncWeatherCSVReader
from boiler.weater_info.io.sync.sync_weather_csv_writer import SyncWeatherCSVWriter
from boiler.weater_info.io.sync.sync_weather_text_file_dumper import SyncWeatherTextFileDumper
from boiler.weater_info.io.sync.sync_weather_text_file_loader import SyncWeatherTextFileLoader
from unittests.weather_sync_dump_load_testing import \
    WeatherSyncDumpLoadTesting


class TestWeatherCSVDumpLoad(WeatherSyncDumpLoadTesting):

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
        return SyncWeatherTextFileLoader(
            filepath=filepath,
            reader=reader
        )

    @pytest.fixture
    def dumper(self, writer, filepath):
        return SyncWeatherTextFileDumper(
            filepath=filepath,
            writer=writer
        )
