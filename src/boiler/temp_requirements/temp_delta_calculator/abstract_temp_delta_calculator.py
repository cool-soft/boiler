import pandas as pd


class AbstractTempDeltaCalculator:

    def calc_temp_delta(self,
                        heating_system_reaction_df: pd.DataFrame,
                        temp_requirements_df: pd.DataFrame
                        ) -> float:
        raise NotImplementedError
