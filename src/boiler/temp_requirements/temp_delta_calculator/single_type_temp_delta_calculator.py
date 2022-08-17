import pandas as pd
from boiler.constants import column_names

from boiler.temp_requirements.temp_delta_calculator.abstract_temp_delta_calculator import AbstractTempDeltaCalculator


class SingleTypeTempDeltaCalculator(AbstractTempDeltaCalculator):

    def calc_temp_delta(self,
                        heating_system_reaction_df: pd.DataFrame,
                        temp_requirements_df: pd.DataFrame
                        ) -> float:
        joined_df = heating_system_reaction_df.join(
            temp_requirements_df.set_index(column_names.TIMESTAMP),
            on=column_names.TIMESTAMP,
            rsuffix="_r"
        )

        forward_temp_notnull_idx = joined_df[column_names.FORWARD_TEMP].notnull()
        forward_temp = joined_df[forward_temp_notnull_idx][column_names.FORWARD_TEMP]
        forward_temp_requirements = joined_df[forward_temp_notnull_idx][f"{column_names.FORWARD_TEMP}_r"]
        joined_df[column_names.FORWARD_TEMP_DELTA] = forward_temp - forward_temp_requirements

        temp_delta = joined_df[column_names.FORWARD_TEMP_DELTA].min()
        return temp_delta
