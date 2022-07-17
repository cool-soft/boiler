from datetime import datetime

import pytest
from dateutil.tz import gettz

from boiler.data_processing.timestamp_parsing_algorithm import SimpleTimestampParsingAlgorithm


class TestDatetimeParsing:

    @pytest.fixture
    def timezone(self):
        return gettz("Asia/Yekaterinburg")

    @pytest.fixture
    def samples(self, timezone):
        return (
            # datetime_as_str, err, result
            ("2022-05-03 23:20", False, datetime(2022, 5, 3, 23, 20, tzinfo=timezone)),
            ("2022-1-2 3:20", False, datetime(2022, 1, 2, 3, 20, tzinfo=timezone)),
            ("20.01.2099 05:17", False, datetime(2099, 1, 20, 5, 17, tzinfo=timezone)),
            ("1.3.2096 4:00", False, datetime(2096, 3, 1, 4, 0, tzinfo=timezone)),
            (".3.2096 4:00", True, None),
        )

    @pytest.fixture
    def parsing_algorithm(self, timezone):
        patterns = [
            r"(?P<year>\d{4})-(?P<month>\d{1,2})-(?P<day>\d{1,2})\s(?P<hours>\d{1,2}):(?P<minutes>\d{2})",
            r"(?P<day>\d{1,2})\.(?P<month>\d{1,2})\.(?P<year>\d{4})\s(?P<hours>\d{1,2}):(?P<minutes>\d{2})"
        ]
        return SimpleTimestampParsingAlgorithm(patterns, timezone)

    def test_datetime_parsing(self, samples, parsing_algorithm):
        for datetime_as_str, err, result in samples:
            if err:
                with pytest.raises(ValueError):
                    parsing_algorithm.parse_datetime(datetime_as_str)
            else:
                assert parsing_algorithm.parse_datetime(datetime_as_str) == result
