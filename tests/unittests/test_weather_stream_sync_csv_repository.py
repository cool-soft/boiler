import pytest

from boiler.weater_info.parsers.weather_data_csv_parser import WeatherDataCSVParser
from boiler.weater_info.repository.stream.sync.weather_stream_sync_csv_repository import WeatherStreamSyncCSVRepository
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
        parser = WeatherDataCSVParser()
        repo = WeatherStreamSyncCSVRepository(filepath, parser, encoding)
        return repo

