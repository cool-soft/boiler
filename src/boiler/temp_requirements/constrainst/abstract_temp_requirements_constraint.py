import pandas as pd


class AbstractTempRequirementsConstraint:

    def check(self,
              control_action_df: pd.DataFrame,
              weather_df: pd.DataFrame
              ) -> bool:
        raise NotImplementedError
