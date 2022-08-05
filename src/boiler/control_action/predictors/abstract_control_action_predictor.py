import pandas as pd


class AbstractControlActionPredictor:

    def predict_one(self,
                    weather_forecast_df: pd.DataFrame,
                    control_action_timestamp: pd.Timestamp
                    ) -> pd.DataFrame:
        raise NotImplementedError
