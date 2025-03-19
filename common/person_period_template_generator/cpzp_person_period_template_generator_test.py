from datetime import datetime
from typing import Optional
from unittest import TestCase
from unittest.mock import Mock, patch

from polars import DataFrame, Series, col

from common.person_period_template_generator.cpzp_person_period_template_generator import (
    CpzpBaseColumn,
    CpzpPersonPeriodTemplateGenerator,
)
from common.person_period_template_generator.person_period_template_generator import PersonPeriodDfSchema
from common.typings import MAX_TIME_PERIOD_VALUE, NewColumn


class TestCpzpPersonPeriodTemplateGenerator(TestCase):
    def setUp(self):
        self.__from_date = datetime(2021, 1, 1)
        self.__to_date = datetime(2022, 12, 31)

    # - When initializing the generator
    # -- It should call TimePeriodHelper and set the weeks correctly
    @patch("common.person_period_template_generator.cpzp_person_period_template_generator.TimePeriodHelper")
    def test_weeks_are_set(self, time_period_helper_mock: Mock) -> None:
        mocked_weeks = ["2021W01", "2021W02", "2021W03"]
        time_period_helper_mock.get_weeks_in_range.return_value = mocked_weeks

        generator = CpzpPersonPeriodTemplateGenerator(self.__from_date, self.__to_date)

        time_period_helper_mock.get_weeks_in_range.assert_called_once_with(self.__from_date, self.__to_date)
        self.assertEqual(generator.weeks, mocked_weeks)
        self.assertEqual(generator.week_indices, {week: i for i, week in enumerate(mocked_weeks)})

    # - When generating the template from a base DataFrame
    # -- It should return a DataFrame with the correct schema
    def test_generate_structure(self) -> None:
        base_df = self.__create_base_df()
        generator = CpzpPersonPeriodTemplateGenerator(self.__from_date, self.__to_date)
        result = generator.generate(base_df)

        self.assertEqual(PersonPeriodDfSchema, result.schema)

    # - When generating the template from a base DataFrame
    # -- It should produce a cross join with the correct number of rows
    def test_row_count(self) -> None:
        amount_of_ppl = 5
        base_df = self.__create_base_df(num_of_rows=amount_of_ppl)
        generator = CpzpPersonPeriodTemplateGenerator(self.__from_date, self.__to_date)
        result = generator.generate(base_df)

        expected_rows = amount_of_ppl * len(generator.weeks)  # (no. of people) * (no. of weeks)
        self.assertEqual(result.height, expected_rows)

    # - When generating the template from a base DataFrame
    # -- It should correctly convert vaccine week strings to dose indices
    def test_dose_conversion(self) -> None:
        dose_weeks = ["2021W10", "2021W11", "2021W12", "2021W13"]

        base_df = self.__create_base_df(
            {
                CpzpBaseColumn.VACCINE_1_DATE: [dose_weeks[0]],
                CpzpBaseColumn.VACCINE_2_DATE: [dose_weeks[1]],
                CpzpBaseColumn.VACCINE_3_DATE: [dose_weeks[2]],
                CpzpBaseColumn.VACCINE_4_DATE: [dose_weeks[3]],
            }
        )

        generator = CpzpPersonPeriodTemplateGenerator(self.__from_date, self.__to_date)
        result = generator.generate(base_df)

        for idx, dose_week in enumerate(dose_weeks):
            expected_index = generator.week_indices.get(dose_week)

            dose_series = self.__get_column_for_person(
                df=result,
                person_id=1,
                col_name=f"dose{idx+1}",  # + 1 cause dose 0 does not exist
            )

            self.assertTrue(all(val == expected_index for val in dose_series))

    # - When generating the template from a base DataFrame
    # -- It should correctly convert the death week to a numeric index (death_index)
    def test_death_index_conversion(self) -> None:
        base_df = self.__create_base_df({CpzpBaseColumn.DEATHDATE: ["2021W20"]})
        generator = CpzpPersonPeriodTemplateGenerator(self.__from_date, self.__to_date)
        result = generator.generate(base_df)

        expected_death_index = generator.week_indices.get("2021W20")
        death_index_series = result.filter(col(NewColumn.PERSON_ID) == 1).select("death_index").to_series()
        self.assertTrue(all(val == expected_death_index for val in death_index_series))

    # - When generating the template from a base DataFrame
    # -- And the person died before the requested time span
    # --- It should set the death index to -1
    def test_death_index_before_requested_time_span(self) -> None:
        base_df = self.__create_base_df({CpzpBaseColumn.DEATHDATE: ["1900W01"]})
        generator = CpzpPersonPeriodTemplateGenerator(self.__from_date, self.__to_date)
        result_df = generator.generate(base_df)

        person_death_indices = self.__get_column_for_person(
            df=result_df,
            person_id=1,
            col_name="death_index",
        )
        self.assertTrue(all(val == -1 for val in person_death_indices))

    # - When generating the template from a base DataFrame
    # -- And the person died after the requested time span
    # --- It should set the death index to maximum value (eg, last week +1)
    def test_death_index_after_requested_time_span(self) -> None:
        base_df = self.__create_base_df({CpzpBaseColumn.DEATHDATE: ["3000W01"]})
        generator = CpzpPersonPeriodTemplateGenerator(self.__from_date, self.__to_date)
        result_df = generator.generate(base_df)

        person_death_indices = self.__get_column_for_person(
            df=result_df,
            person_id=1,
            col_name="death_index",
        )
        self.assertTrue(all(val == MAX_TIME_PERIOD_VALUE for val in person_death_indices))

    # - When generating the template from a base DataFrame
    # -- And the person did not died at all
    # --- It should set the death index to maximum value (eg, last week +1)
    def test_death_index_not_set(self) -> None:
        base_df = self.__create_base_df()
        generator = CpzpPersonPeriodTemplateGenerator(self.__from_date, self.__to_date)
        result_df = generator.generate(base_df)

        person_death_indices = self.__get_column_for_person(
            df=result_df,
            person_id=1,
            col_name="death_index",
        )
        self.assertTrue(all(val == MAX_TIME_PERIOD_VALUE for val in person_death_indices))

    def __get_column_for_person(self, df: DataFrame, person_id: int, col_name: str) -> Series:
        return df.filter(col(NewColumn.PERSON_ID) == person_id).select(col_name).to_series()

    def __create_base_df(
        self,
        overrides: Optional[dict[CpzpBaseColumn, list[str]]] = None,
        num_of_rows: int = 1,
    ) -> DataFrame:
        if overrides is None:
            overrides = {}

        if len(overrides):
            num_of_rows = max(len(v) for v in overrides.values())

        base_data = {col.value: [""] * num_of_rows for col in CpzpBaseColumn}

        for key, values in overrides.items():
            base_data[key.value] = values

        return DataFrame(base_data)
