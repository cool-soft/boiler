import re
from datetime import datetime, tzinfo
from typing import List


class AbstractTimestampParsingAlgorithm:

    def parse_datetime(self, datetime_str: str) -> datetime:
        raise NotImplementedError


class SimpleTimestampParsingAlgorithm(AbstractTimestampParsingAlgorithm):

    def __init__(self,
                 datetime_patterns: List[str],
                 timezone: tzinfo
                 ) -> None:
        self._datetime_parsers = []
        for pattern in datetime_patterns:
            parser = re.compile(pattern)
            self._datetime_parsers.append(parser)
        self._timezone = timezone

    def parse_datetime(self,
                       datetime_str: str
                       ) -> datetime:
        for parser in self._datetime_parsers:
            parsed = parser.match(datetime_str)
            if parsed is not None:
                break
        else:
            raise ValueError("Date and time are not matched using existing patterns")
        year = int(parsed.group("year"))
        month = int(parsed.group("month"))
        day = int(parsed.group("day"))
        hour = int(parsed.group("hours"))
        minute = int(parsed.group("minutes"))
        second = 0
        millisecond = 0

        datetime_ = datetime(year, month, day, hour, minute, second, millisecond, tzinfo=self._timezone)
        return datetime_
