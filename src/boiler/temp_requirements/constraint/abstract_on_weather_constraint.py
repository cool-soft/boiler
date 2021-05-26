import pandas as pd


class AbstractOnWeatherConstraint:

    def check(self,
              system_reaction_df: pd.DataFrame,
              weather_df: pd.DataFrame
              ) -> float:
        raise NotImplementedError
