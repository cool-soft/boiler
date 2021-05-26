import pandas as pd


class AbstractTempRequirementsConstraint:

    def check(self,
              system_reaction_df: pd.DataFrame,
              weather_df: pd.DataFrame
              ) -> float:
        raise NotImplementedError
