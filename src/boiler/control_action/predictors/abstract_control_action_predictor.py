import pandas as pd


class AbstractControlActionPredictor:

    def predict_on_weather(self,
                           weather_forecast_df: pd.DataFrame
                           ) -> pd.DataFrame:
        raise NotImplementedError
