import numpy as np
import pandas as pd


class AbstractSmoothAlgorithm:

    def smooth_series(self, x: pd.Series) -> pd.Series:
        raise NotImplementedError


class NumpyBasedSmoothAlgorithm(AbstractSmoothAlgorithm):

    def __init__(self, window_type: str = 'hanning', window_len: int = 4) -> None:
        if window_type not in ['flat', 'hanning', 'hamming', 'bartlett', 'blackman']:
            raise ValueError("Window is on of 'flat', 'hanning', 'hamming', 'bartlett', 'blackman'")
        if window_type == 'flat':
            self._window_processor = np.ones(window_len, 'd')
        else:
            self._window_processor = getattr(np, window_type)(window_len)
        self._window_len = window_len

    def smooth_series(self, x: pd.Series) -> pd.Series:
        if x.ndim != 1:
            raise ValueError("smooth only accepts 1 dimension arrays.")
        if x.size < self._window_len:
            raise ValueError("Input vector needs to be bigger than window size.")
        if self._window_len < 3:
            return x
        s = np.r_[x[self._window_len - 1:0:-1], x, x[-2:-self._window_len - 1:-1]]
        y = np.convolve(self._window_processor / self._window_processor.sum(), s, mode='valid')
        y = y[(self._window_len // 2 - 1 + (self._window_len % 2)):-(self._window_len // 2)]
        return pd.Series(y)
