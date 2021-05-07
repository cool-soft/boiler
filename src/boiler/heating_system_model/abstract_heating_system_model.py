from typing import List

from .heating_system_reaction import HeatingSystemReaction


class AbstractHeatingSystemModel:

    def predict_on_boiler_temp(self, boiler_temp: float) -> List[HeatingSystemReaction]:
        raise NotImplementedError
