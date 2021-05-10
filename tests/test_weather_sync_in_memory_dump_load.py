import pytest

from boiler.weather.io.sync_weather_in_memory_dumper_loader import SyncWeatherInMemoryDumperLoader
from weather_sync_dump_load_testing import WeatherSyncDumpLoadTesting


class TestWeatherSyncPickleDumpLoad(WeatherSyncDumpLoadTesting):

    @pytest.fixture
    def in_memory_dumper_loader(self):
        return SyncWeatherInMemoryDumperLoader()

    @pytest.fixture
    def loader(self, in_memory_dumper_loader):
        return in_memory_dumper_loader

    @pytest.fixture
    def dumper(self, in_memory_dumper_loader):
        return in_memory_dumper_loader
