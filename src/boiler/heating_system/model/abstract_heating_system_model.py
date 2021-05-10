import pandas as pd


class AbstractHeatingSystemModel:

    def predict(self,
                weather_df: pd.DataFrame,
                system_state_history_df: pd.DataFrame,
                control_action_df: pd.DataFrame
                ) -> pd.DataFrame:
        raise NotImplementedError
