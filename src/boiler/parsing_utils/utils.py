import numpy as np
import pandas as pd

from boiler.constants import column_names


def average_values(x: np.array, window_len: int = 4, window: str = 'hanning') -> np.array:
    if x.ndim != 1:
        raise ValueError("smooth only accepts 1 dimension arrays.")

    if x.size < window_len:
        raise ValueError("Input vector needs to be bigger than window size.")

    if window not in ['flat', 'hanning', 'hamming', 'bartlett', 'blackman']:
        raise ValueError("Window is on of 'flat', 'hanning', 'hamming', 'bartlett', 'blackman'")

    if window_len < 3:
        return x

    s = np.r_[x[window_len - 1:0:-1], x, x[-2:-window_len - 1:-1]]

    if window == 'flat':
        w = np.ones(window_len, 'd')
    else:
        w = getattr(np, window)(window_len)

    y = np.convolve(w / w.sum(), s, mode='valid')
    return y[(window_len // 2 - 1 + (window_len % 2)):-(window_len // 2)]
    # return y


def filter_by_timestamp_closed(df: pd.Dataframe, start_datetime, end_datetime) -> pd.DataFrame:
    df = df[
        (df[column_names.TIMESTAMP] >= start_datetime) &
        (df[column_names.TIMESTAMP] <= end_datetime)
    ]
    return df
