from random import random

import pandas as pd
import pytest
from dateutil.tz import tzlocal

from boiler.constants import column_names, time_tick


# noinspection PyMethodMayBeStatic
class WeatherSyncDumpLoadTesting:

    @pytest.fixture
    def time_tick_(self):
        return time_tick.TIME_TICK

    @pytest.fixture
    def weather_info(self, time_tick_):
        weather_df = pd.DataFrame(
            columns=(column_names.TIMESTAMP,
                     column_names.WEATHER_TEMP)
        )

        weather_df_len = 5
        start_datetime = pd.Timestamp.now(tz=tzlocal())
        end_datetime = start_datetime + (weather_df_len * time_tick_)

        weather_data_to_append = []
        current_datetime = start_datetime
        while current_datetime <= end_datetime:
            weather_data_to_append.append({
                column_names.TIMESTAMP: current_datetime,
                column_names.WEATHER_TEMP: random()
            })
            current_datetime += time_tick_
        weather_df = weather_df.append(weather_data_to_append)

        return weather_df

    def test_weather_stream_sync_repository_set_get(self, weather_info, dumper, loader):
        dumper.dump_weather(weather_info)
        loaded_weather_info = loader.load_weather()
        weather_info[column_names.WEATHER_TEMP] = weather_info[column_names.WEATHER_TEMP].round(2)
        loaded_weather_info[column_names.WEATHER_TEMP] = loaded_weather_info[column_names.WEATHER_TEMP].round(2)
        assert loaded_weather_info.to_dict("records") == weather_info.to_dict("records")
