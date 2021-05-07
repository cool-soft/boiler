import pandas as pd


class AbstractTempRequirementsPredictor:

    def predict_on_weather(self, weather_df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError
