from boiler.heating_obj.processing.heating_obj_processors\
    import AbstractAsyncHeatingObjProcessor, AbstractSyncHeatingObjProcessor


class AbstractHeatingObjProcessorFactory:

    def create_async_processor_for_timestamp_range(self,
                                                   min_timestamp,
                                                   max_timestamp) -> AbstractAsyncHeatingObjProcessor:
        raise NotImplementedError

    def create_sync_processor_for_timestamp_range(self,
                                                  min_timestamp,
                                                  max_timestamp) -> AbstractSyncHeatingObjProcessor:
        raise NotImplementedError

    def create_async_processor(self) -> AbstractAsyncHeatingObjProcessor:
        raise NotImplementedError

    def create_sync_processor(self) -> AbstractSyncHeatingObjProcessor:
        raise NotImplementedError
