from unittest import TestCase

from polars import DataFrame

from common.person_period_processor.person_period_template_processor import PersonPeriodTemplateProcessor
from common.person_period_template_generator.person_period_template_generator import PersonPeriodDfSchema, TempColumn
from common.typings import NewColumn


class TestPersonPeriodTemplateProcessor(TestCase):
    def setUp(self) -> None:
        self.processor = PersonPeriodTemplateProcessor()

    # - Given a DataFrame with the correct schema
    # -- When calling the process method with default parameters
    # -- It should return a DataFrame with the required columns
    def test_process_should_return_dataframe_with_required_columns(self):
        df = DataFrame(
            {
                TempColumn.PERSON_ID: [1, 1],
                TempColumn.BIRTHDATE: ["2000W01", "2000W01"],
                TempColumn.DOSE_1: [1, 1],
                TempColumn.DOSE_2: [2, 2],
                TempColumn.DOSE_3: [3, 3],
                TempColumn.DOSE_4: [4, 4],
                TempColumn.DEATH_INDEX: [1, 2],
                TempColumn.TIME_PERIOD: ["2021W01", "2021W02"],
                TempColumn.TIME_PERIOD_INDEX: [1, 2],
            },
            schema=PersonPeriodDfSchema,
        )

        result_df = self.processor.process(df)

        self.assertTrue(all(col in result_df.columns for col in NewColumn))

    # - Given a DataFrame with incorrect schema
    # -- When calling the process method
    # -- It should raise an assertion error due to schema mismatch
    def test_process_should_raise_assertion_error_for_invalid_schema(self):
        df_invalid = DataFrame(
            {
                "invalid_column": [1, 2, 3],
            }
        )

        with self.assertRaises(AssertionError):
            self.processor.process(df_invalid)

    # Add tests for checking needed expression calls and col selection
