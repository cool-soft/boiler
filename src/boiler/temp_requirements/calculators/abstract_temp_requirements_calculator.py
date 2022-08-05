import pandas as pd


class AbstractTempRequirementsCalculator:

    def calc_for_weather(self, weather_df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError
