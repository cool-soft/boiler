from typing import Union

import pandas as pd
from dataclasses import dataclass


@dataclass
class HeatingSystemReaction:
    timedelta: pd.Timedelta
    object_id: str
    object_type: str
    forward_pipe_coolant_temp: Union[float, int, None]
    backward_pipe_coolant_temp: Union[float, int, None]
