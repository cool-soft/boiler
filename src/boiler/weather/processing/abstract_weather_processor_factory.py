from boiler.weather.processing.weather_processors \
    import AbstractSyncWeatherProcessor, AbstractAsyncWeatherProcessor


class AbstractWeatherProcessorFactory:

    def create_async_processor_for_timestamp_range(self,
                                                   min_timestamp,
                                                   max_timestamp) -> AbstractAsyncWeatherProcessor:
        raise NotImplementedError

    def create_sync_processor_for_timestamp_range(self,
                                                  min_timestamp,
                                                  max_timestamp) -> AbstractSyncWeatherProcessor:
        raise NotImplementedError

    def create_async_processor(self) -> AbstractAsyncWeatherProcessor:
        raise NotImplementedError

    def create_sync_processor(self) -> AbstractSyncWeatherProcessor:
        raise NotImplementedError
