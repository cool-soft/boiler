import pytest

from boiler.weater_info.repository.stream.sync.weather_stream_sync_pickle_repository import \
    WeatherStreamSyncPickleRepository
from unittests.weather_stream_sync_repository_base_operations_testing import \
    WeatherStreamSyncRepositoryBaseOperationsTesting


class TestWeatherStreamSyncPickleRepository(WeatherStreamSyncRepositoryBaseOperationsTesting):

    @pytest.fixture
    def filepath(self, tmp_path):
        return tmp_path / "weather.pickle"

    @pytest.fixture
    def repository(self, filepath):
        repo = WeatherStreamSyncPickleRepository(filepath)
        return repo
