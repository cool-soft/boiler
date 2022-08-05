import pandas as pd
from boiler.constants import column_names

from boiler.control_action.temp_delta_calculator.abstract_temp_delta_calculator import AbstractTempDeltaCalculator


class SingleTypeTempDeltaCalculator(AbstractTempDeltaCalculator):

    def calc_temp_delta(self,
                        heating_system_reaction_df: pd.DataFrame,
                        temp_requirements_df: pd.DataFrame
                        ) -> pd.DataFrame:
        joined_df = heating_system_reaction_df.join(
            temp_requirements_df.set_index(column_names.TIMESTAMP),
            on=column_names.TIMESTAMP,
            rsuffix="_r"
        )

        forward_temp_notnull_idx = joined_df[column_names.FORWARD_TEMP].notnull()
        joined_df[column_names.FORWARD_TEMP_DELTA] = \
            joined_df[forward_temp_notnull_idx][column_names.FORWARD_TEMP] - \
            joined_df[forward_temp_notnull_idx][f"{column_names.FORWARD_TEMP}_r"]

        backward_temp_notnull_idx = joined_df[column_names.BACKWARD_TEMP].notnull()
        joined_df[column_names.BACKWARD_TEMP_DELTA] = \
            joined_df[backward_temp_notnull_idx][column_names.BACKWARD_TEMP] - \
            joined_df[backward_temp_notnull_idx][f"{column_names.BACKWARD_TEMP}_r"]

        temp_delta_df = joined_df[
            [
                column_names.TIMESTAMP,
                column_names.FORWARD_TEMP_DELTA,
                column_names.BACKWARD_TEMP_DELTA
            ]
        ].copy()
        return temp_delta_df
