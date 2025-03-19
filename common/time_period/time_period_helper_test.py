from datetime import datetime
from unittest import TestCase

from common.time_period.time_period_helper import TimePeriodHelper


class TestTimePeriodHelper(TestCase):
    # - When the start date is January 1st
    # -- It should correctly handle ISO week from the previous year
    def test_first_day_of_year(self):
        result = self.__generate_weeks("2021-01-01", "2021-01-01")
        self.assertEqual(result, ["2020W53"])

    # - When generating weeks in a full LEAP year
    # -- It should return 53 weeks
    def test_full_leap_year(self):
        result = self.__generate_weeks("2020-01-01", "2020-12-31")
        self.assertEqual(len(result), 53)

    # - When generating weeks in a full year
    # -- But the start date still belongs to the previous year's week
    # --- It should return 53 weeks
    def test_weeks_full_year(self):
        result = self.__generate_weeks("2021-01-01", "2021-12-31")
        self.assertEqual(len(result), 53)
        self.assertEqual(result[0], "2020W53")

    # - When generating weeks across two years
    # -- It should correctly handle the year transition
    def test_weeks_cross_year(self):
        result = self.__generate_weeks("2021-12-28", "2022-01-14")
        self.assertEqual(result, ["2021W52", "2022W01", "2022W02"])

    # - When asked to generate weeks in a range
    # -- And the end date is before the start date
    # --- It should return an empty list
    def test_weeks_invalid_range(self):
        result = self.__generate_weeks("2022-12-31", "2021-01-01")
        self.assertEqual(result, [])

    # - When asked for months in a full year range
    # -- It should return exactly 12 months
    def test_months_full_year(self):
        result = self.__generate_months("2021-01-01", "2021-12-31")
        self.assertEqual(len(result), 12)
        self.assertIn("2021M01", result)
        self.assertIn("2021M12", result)

    # - When asked for months across two years
    # -- It should handle year transition correctly
    def test_months_cross_year(self):
        result = self.__generate_months("2021-11-01", "2022-02-28")
        self.assertEqual(result, ["2021M11", "2021M12", "2022M01", "2022M02"])

    # - When asked for months for a single month range
    # -- It should return exactly one month
    def test_one_month(self):
        result = self.__generate_months("2022-08-01", "2022-08-31")
        self.assertEqual(result, ["2022M08"])

    # - When asked for months for multiple years
    # -- It should return all months correctly
    def test_longer_range(self):
        result = self.__generate_months("2020-01-01", "2022-12-31")
        self.assertEqual(len(result), 36)  # 3 years * 12 months

    # -  When asked for months
    # -- And the end date is before the start date
    # --- It should return an empty list
    def test_months_invalid_range(self):
        result = self.__generate_months("2022-12-31", "2021-01-01")
        self.assertEqual(result, [])

    def __generate_weeks(self, start_date: str, end_date: str) -> list[str]:
        return TimePeriodHelper.get_weeks_in_range(datetime.fromisoformat(start_date), datetime.fromisoformat(end_date))

    def __generate_months(self, start_date: str, end_date: str) -> list[str]:
        return TimePeriodHelper.get_months_in_range(datetime.fromisoformat(start_date), datetime.fromisoformat(end_date))
