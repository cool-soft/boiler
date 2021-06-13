import pytest
from dateutil.tz import gettz
from datetime import datetime

from boiler.data_processing.other import parse_datetime


class TestDatetimeParsing:

    @pytest.fixture
    def patterns(self):
        return (
            r"(?P<year>\d{4})-(?P<month>\d{1,2})-(?P<day>\d{1,2})\s(?P<hours>\d{1,2}):(?P<minutes>\d{2})",
            r"(?P<day>\d{1,2})\.(?P<month>\d{1,2})\.(?P<year>\d{4})\s(?P<hours>\d{1,2}):(?P<minutes>\d{2})"
        )

    @pytest.fixture
    def samples(self):
        return (
            ("2022-05-03 23:20", False, datetime(2022, 5, 3, 23, 20, tzinfo=gettz("Asia/Yekaterinburg"))),
            ("2022-1-2 3:20", False, datetime(2022, 1, 2, 3, 20, tzinfo=gettz("Europe/Moscow"))),
            ("20.01.2099 05:17", False, datetime(2099, 1, 20, 5, 17, tzinfo=gettz("Europe/Moscow"))),
            ("1.3.2096 4:00", False, datetime(2096, 3, 1, 4, 0, tzinfo=gettz("Asia/Yekaterinburg"))),
            (".3.2096 4:00", True, None),
        )

    def test_datetime_parsing(self, samples, patterns):
        for datetime_as_str, err, result in samples:
            if err:
                with pytest.raises(ValueError):
                    parse_datetime(datetime_as_str, patterns, None)
            else:
                assert parse_datetime(datetime_as_str, patterns, result.tzinfo) == result
