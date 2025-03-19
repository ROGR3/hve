from unittest import TestCase

import pytest
from polars import DataFrame

from common.person_period_template_generator.person_period_template_generator import TempColumn
from common.polars_expressions.age_group_expression import AgeGroupExpression
from common.typings import AgeStatus


class TestAgeGroupExpression(TestCase):
    # - When using the age group expression
    # -- And the computed age is 59
    # --- It should be classified as LESS_THAN_60
    def test_age_less_than_60(self):
        df = DataFrame(
            {
                TempColumn.TIME_PERIOD: ["2021W01"],
                TempColumn.BIRTHDATE: ["1961W02"],
            }
        )
        result = df.with_columns(AgeGroupExpression.get_expr().alias("age_group"))
        self.assertEqual(result["age_group"].to_list(), [AgeStatus.LESS_THAN_60])

    # - When using the age group expression
    # -- And the computed age is exactly 60
    # --- It should be classified as BETWEEN_60_AND_69
    def test_age_exactly_60(self):
        df = DataFrame(
            {
                TempColumn.TIME_PERIOD: ["2021W06"],
                TempColumn.BIRTHDATE: ["1961W06"],
            }
        )
        result = df.with_columns(AgeGroupExpression.get_expr().alias("age_group"))
        self.assertEqual(result["age_group"].to_list(), [AgeStatus.BETWEEN_60_AND_69])

    # - When using the age group expression
    # -- And the computed age is 69 (just under 70)
    # --- It should be classified as BETWEEN_60_AND_69
    def test_age_just_under_70(self):
        df = DataFrame(
            {
                TempColumn.TIME_PERIOD: ["2030W01"],
                TempColumn.BIRTHDATE: ["1960W06"],
            }
        )
        result = df.with_columns(AgeGroupExpression.get_expr().alias("age_group"))
        self.assertEqual(result["age_group"].to_list(), [AgeStatus.BETWEEN_60_AND_69])

    # - When using the age group expression
    # -- And the computed age is exactly 70
    # --- It should be classified as BETWEEN_70_AND_79
    def test_age_exactly_70(self):
        df = DataFrame(
            {
                TempColumn.TIME_PERIOD: ["2030W06"],
                TempColumn.BIRTHDATE: ["1960W06"],
            }
        )
        result = df.with_columns(AgeGroupExpression.get_expr().alias("age_group"))
        self.assertEqual(result["age_group"].to_list(), [AgeStatus.BETWEEN_70_AND_79])

    # - When using the age group expression
    # -- And the computed age is 79 (just under 80)
    # --- It should be classified as BETWEEN_70_AND_79
    def test_age_just_under_80(self):
        df = DataFrame(
            {
                TempColumn.TIME_PERIOD: ["2040W01"],
                TempColumn.BIRTHDATE: ["1960W06"],
            }
        )
        result = df.with_columns(AgeGroupExpression.get_expr().alias("age_group"))
        self.assertEqual(result["age_group"].to_list(), [AgeStatus.BETWEEN_70_AND_79])

    # - When using the age group expression
    # -- And the computed age is exactly 80
    # --- It should be classified as GREATER_THAN_80
    def test_age_exactly_80(self):
        df = DataFrame(
            {
                TempColumn.TIME_PERIOD: ["2040W06"],
                TempColumn.BIRTHDATE: ["1960W06"],
            }
        )
        result = df.with_columns(AgeGroupExpression.get_expr().alias("age_group"))
        self.assertEqual(result["age_group"].to_list(), [AgeStatus.GREATER_THAN_80])

    # - When using the age group expression
    # -- And the birthdate is in the future (negative age)
    # --- It should be classified as LESS_THAN_60
    def test_negative_age(self):
        df = DataFrame(
            {
                TempColumn.TIME_PERIOD: ["2020W01"],
                TempColumn.BIRTHDATE: ["2025W06"],
            }
        )
        result = df.with_columns(AgeGroupExpression.get_expr().alias("age_group"))
        self.assertEqual(result["age_group"].to_list(), [AgeStatus.LESS_THAN_60])

    # - When using the age group expression
    # -- And the birthdate is null
    # --- It should raise an Exception
    def test_null_values(self):
        df = DataFrame(
            {
                TempColumn.TIME_PERIOD: ["2021W06"],
                TempColumn.BIRTHDATE: [None],
            }
        )

        with pytest.raises(Exception) as exc:
            df.with_columns(AgeGroupExpression.get_expr().alias("age_group"))

        self.assertIsInstance(exc.value, Exception)
        self.assertTrue("expected `String`, got `null`" in str(exc.value))

    # - When using the age group expression
    # -- And the date format is invalid
    # --- It should raise an Exception
    def test_invalid_date_format(self):
        df = DataFrame(
            {
                TempColumn.TIME_PERIOD: ["2021W06"],
                TempColumn.BIRTHDATE: ["not-a-date"],
            }
        )

        with pytest.raises(Exception) as exc:
            df.with_columns(AgeGroupExpression.get_expr().alias("age_group"))

        self.assertIsInstance(exc.value, Exception)
        self.assertTrue("conversion from `str` to `i64` failed in column 'tyden_narozeni'" in str(exc.value))
