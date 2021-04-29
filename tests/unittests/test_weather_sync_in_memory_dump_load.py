import pytest

from boiler.constants import column_names
from boiler.data_processing.processing_algo.beetween_filter_algorithm import FullClosedBetweenFilterAlgorithm
from boiler.weather.io.sync.sync_weather_in_memory_dumper_loader import SyncWeatherInMemoryDumperLoader
from unittests.weather_sync_dump_load_testing import WeatherSyncDumpLoadTesting


class TestWeatherSyncPickleDumpLoad(WeatherSyncDumpLoadTesting):

    @pytest.fixture
    def filter_algorithm(self):
        return FullClosedBetweenFilterAlgorithm(column_name=column_names.TIMESTAMP)

    @pytest.fixture
    def in_memory_dumper_loader(self, filter_algorithm):
        return SyncWeatherInMemoryDumperLoader(filter_algorithm=filter_algorithm)

    @pytest.fixture
    def loader(self, in_memory_dumper_loader):
        return in_memory_dumper_loader

    @pytest.fixture
    def dumper(self, in_memory_dumper_loader):
        return in_memory_dumper_loader
