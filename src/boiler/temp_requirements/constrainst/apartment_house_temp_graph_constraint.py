import logging

import pandas as pd

from boiler.constants import heating_object_types, column_names
from boiler.data_processing.timestamp_round_algorithm import AbstractTimestampRoundAlgorithm
from boiler.temp_requirements.predictors.abstract_temp_requirements_predictor \
    import AbstractTempRequirementsPredictor
from .abstract_temp_requirements_constraint import AbstractTempRequirementsConstraint


class SingleHeatingObjTypeSimpleConstraint(AbstractTempRequirementsConstraint):

    def __init__(self,
                 temp_requirements_predictor: AbstractTempRequirementsPredictor,
                 timestamp_round_algo: AbstractTimestampRoundAlgorithm,
                 temp_requirements_coefficient: float = 1.0,
                 min_model_error: float = 1.0,
                 heating_obj_type: str = heating_object_types.APARTMENT_HOUSE
                 ) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug("Creating instance")

        self._temp_requirements_coefficient = temp_requirements_coefficient
        self._temp_requirements_predictor = temp_requirements_predictor
        self._model_error = min_model_error
        self._timestamp_round_algorithm = timestamp_round_algo
        self._heating_obj_type = heating_obj_type

    def check(self,
              system_reaction_df: pd.DataFrame,
              weather_df: pd.DataFrame
              ) -> bool:
        system_reaction_df = self._filter_reaction_by_heating_obj_type(system_reaction_df)
        system_reaction_df = self._round_timestamp(system_reaction_df)
        # TODO: обрезать из weather_df только нужную для сравнения часть по TIMESTAMP
        weather_df = self._round_timestamp(weather_df)
        temp_requirements_df = self._temp_requirements_predictor.predict_on_weather(weather_df)
        for column_name in (column_names.FORWARD_PIPE_COOLANT_TEMP,
                            column_names.BACKWARD_PIPE_COOLANT_TEMP):
            if not self._compare_by_column(system_reaction_df, temp_requirements_df, column_name):
                return False
        return True

    def _round_timestamp(self,
                         df: pd.DataFrame
                         ) -> pd.DataFrame:
        df = df.copy()
        df[column_names.TIMESTAMP] = \
            self._timestamp_round_algorithm.round_series(df[column_names.TIMESTAMP])
        return df

    def _filter_reaction_by_heating_obj_type(self,
                                             system_reaction_df: pd.DataFrame
                                             ) -> pd.DataFrame:
        system_reaction_df = system_reaction_df[
            system_reaction_df[column_names.HEATING_OBJ_TYPE] == self._heating_obj_type
            ].copy()
        return system_reaction_df

    def _compare_by_column(self,
                           predicted_df: pd.DataFrame,
                           required_df: pd.DataFrame,
                           column_to_compare: str
                           ) -> bool:
        predicted_column = "PREDICTED"
        required_column = "REQUIRED"

        required_df = required_df[[column_names.TIMESTAMP, column_to_compare]].copy()
        required_df = required_df.rename({column_to_compare: required_column})
        required_df[required_column] = required_df[required_column] * self._temp_requirements_coefficient

        predicted_df = predicted_df[[column_names.TIMESTAMP, column_to_compare]].copy()
        predicted_df = predicted_df[predicted_df[column_to_compare].notnull()]
        predicted_df = predicted_df.rename({column_to_compare: predicted_column})
        predicted_df[predicted_column] = predicted_df[predicted_column] - self._model_error

        combined_df = predicted_df.merge(required_df, how="left", on=column_names.TIMESTAMP)
        # noinspection PyUnresolvedReferences
        check = (combined_df[predicted_column] >= combined_df[required_column]).all()
        return check
