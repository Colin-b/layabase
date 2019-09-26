import datetime

_date_time_for_tests = datetime.datetime(2018, 10, 11, 15, 5, 5, 663979)


class DateTimeModuleMock:
    class DateTimeMock:
        @staticmethod
        def utcnow():
            return _date_time_for_tests

    datetime = DateTimeMock
