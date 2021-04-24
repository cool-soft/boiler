import pytest

from boiler.weater_info.io.sync.sync_weather_binary_file_loader import \
    SyncWeatherBinaryFileDumper
from unittests.weather_stream_sync_repository_base_operations_testing import \
    WeatherStreamSyncRepositoryBaseOperationsTesting


class TestWeatherStreamSyncPickleRepository(WeatherStreamSyncRepositoryBaseOperationsTesting):

    @pytest.fixture
    def filepath(self, tmp_path):
        return tmp_path / "weather.pickle"

    @pytest.fixture
    def repository(self, filepath):
        repo = SyncWeatherBinaryFileDumper(filepath)
        return repo
