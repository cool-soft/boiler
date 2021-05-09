import pandas as pd


class AbstractHeatingSystemModel:

    # TODO: добавить параметр погоду, нужен для моделей, чье поведение зависит от погоды
    def predict_on_boiler_temp(self, boiler_temp: float) -> pd.DatFrame:
        raise NotImplementedError
