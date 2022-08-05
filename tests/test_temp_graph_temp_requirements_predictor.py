import pandas as pd
import pytest
from dateutil.tz import gettz

from boiler.constants import column_names, dataset_prototypes
from boiler.data_processing.float_round_algorithm import ArithmeticFloatRoundAlgorithm
from boiler.temp_requirements.calculators.temp_graph_requirements_calculator import TempGraphRequirementsCalculator


class TestTempGraphTempRequirementsPredictor:

    start_timestamp = pd.Timestamp.now(tz=gettz("Asia/Yekaterinburg"))
    time_step = pd.Timedelta(seconds=600)

    @pytest.fixture
    def temp_graph_df(self):
        return pd.DataFrame([
            {
                column_names.WEATHER_TEMP: 5.0,
                column_names.FORWARD_TEMP: 20.0,
                column_names.BACKWARD_TEMP: 15.0
            },
            {
                column_names.WEATHER_TEMP: 0.0,
                column_names.FORWARD_TEMP: 25.0,
                column_names.BACKWARD_TEMP: 20.0
            },
            {
                column_names.WEATHER_TEMP: -5.0,
                column_names.FORWARD_TEMP: 30.0,
                column_names.BACKWARD_TEMP: 25.0
            },
        ])

    @pytest.fixture
    def weather_and_answers_df(self):
        data_list = [
            {
                column_names.WEATHER_TEMP: 10.0,
                column_names.FORWARD_TEMP: 20.0,
                column_names.BACKWARD_TEMP: 15.0
            },
            {
                column_names.WEATHER_TEMP: 5.0,
                column_names.FORWARD_TEMP: 20.0,
                column_names.BACKWARD_TEMP: 15.0
            },
            {
                column_names.WEATHER_TEMP: 4.5,
                column_names.FORWARD_TEMP: 20.0,
                column_names.BACKWARD_TEMP: 15.0
            },
            {
                column_names.WEATHER_TEMP: 4.4,
                column_names.FORWARD_TEMP: 25.0,
                column_names.BACKWARD_TEMP: 20.0
            },
            {
                column_names.WEATHER_TEMP: 0.0,
                column_names.FORWARD_TEMP: 25.0,
                column_names.BACKWARD_TEMP: 20.0
            },
            {
                column_names.WEATHER_TEMP: -0.4,
                column_names.FORWARD_TEMP: 25.0,
                column_names.BACKWARD_TEMP: 20.0
            },
            {
                column_names.WEATHER_TEMP: -0.5,
                column_names.FORWARD_TEMP: 30.0,
                column_names.BACKWARD_TEMP: 25.0
            },
            {
                column_names.WEATHER_TEMP: -5.0,
                column_names.FORWARD_TEMP: 30.0,
                column_names.BACKWARD_TEMP: 25.0
            },
            {
                column_names.WEATHER_TEMP: -10.0,
                column_names.FORWARD_TEMP: 30.0,
                column_names.BACKWARD_TEMP: 25.0
            }
        ]
        for i, data in enumerate(data_list):
            data[column_names.TIMESTAMP] = self.start_timestamp + (i * self.time_step)
        return pd.DataFrame(data_list)

    @pytest.fixture
    def answers_df(self, weather_and_answers_df):
        return weather_and_answers_df[[
            column_names.TIMESTAMP,
            column_names.FORWARD_TEMP,
            column_names.BACKWARD_TEMP
        ]].copy()

    @pytest.fixture
    def weather_df(self, weather_and_answers_df):
        return weather_and_answers_df[
            list(dataset_prototypes.WEATHER.columns)
        ].copy()

    @pytest.fixture
    def predictor(self, temp_graph_df):
        return TempGraphRequirementsCalculator(
            temp_graph_df,
            ArithmeticFloatRoundAlgorithm(decimals=0)
        )

    def test_prediction(self, weather_df, predictor, answers_df):
        temp_requirements_df = predictor.calc_for_weather(weather_df)
        assert temp_requirements_df.to_dict("records") == answers_df.to_dict("records")
