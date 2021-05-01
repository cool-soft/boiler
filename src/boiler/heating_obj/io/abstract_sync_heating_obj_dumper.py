import pandas as pd


class AbstractSyncHeatingObjDumper:

    def dump_heating_obj(self,
                         heating_obj_df: pd.DataFrame) -> None:
        raise NotImplementedError
