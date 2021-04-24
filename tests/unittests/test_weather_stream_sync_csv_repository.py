import pytest

from boiler.weater_info.io.sync.sync_weather_csv_reader import SyncWeatherCSVReader
from boiler.weater_info.io.sync.sync_weather_text_file_loader import SyncWeatherTextFileLoader
from unittests.weather_stream_sync_repository_base_operations_testing import \
    WeatherStreamSyncRepositoryBaseOperationsTesting


class TestWeatherStreamSyncCSVRepository(WeatherStreamSyncRepositoryBaseOperationsTesting):

    @pytest.fixture
    def filepath(self, tmp_path):
        return tmp_path / "weather.csv"

    @pytest.fixture
    def encoding(self):
        return "utf-8"

    @pytest.fixture
    def repository(self, filepath, encoding):
        parser = SyncWeatherCSVReader()
        repo = SyncWeatherTextFileLoader(filepath, parser, encoding)
        return repo

