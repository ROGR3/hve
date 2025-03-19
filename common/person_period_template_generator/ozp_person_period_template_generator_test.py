from datetime import datetime
from typing import Optional
from unittest import TestCase

from polars import DataFrame, Series, Utf8, col

from common.person_period_template_generator.ozp_person_period_template_generator import (
    OzpBaseColumn,
    OzpBaseDfSchema,
    OzpPersonPeriodTemplateGenerator,
)
from common.person_period_template_generator.person_period_template_generator import PersonPeriodDfSchema, TempColumn
from common.typings import MAX_TIME_PERIOD_VALUE


class TestOzpPersonPeriodTemplateGenerator(TestCase):
    def setUp(self):
        self.__from_date = datetime(2021, 1, 1)
        self.__to_date = datetime(2022, 12, 31)

        self.__generator = OzpPersonPeriodTemplateGenerator(self.__from_date, self.__to_date)

    # - When generating the template from a base DataFrame
    # -- It should return a DataFrame with the correct schema
    def test_generate_structure(self):
        base_df = self.__create_base_df()
        result = self.__generator.generate(base_df)

        self.assertEqual(PersonPeriodDfSchema, result.schema)

    # - When generating the template from a base DataFrame
    # -- It should produce a cross join with the correct number of rows
    def test_row_count(self):
        amount_of_ppl = 5
        base_df = self.__create_base_df(num_of_rows=amount_of_ppl)
        result = self.__generator.generate(base_df)

        expected_rows = amount_of_ppl * 24  # (no. of people) * (no. of months)
        self.assertEqual(result.height, expected_rows)

    # - When generating the template from a base DataFrame
    # -- It should correctly convert vaccine month strings to dose indices
    def test_dose_conversion(self):
        dose_years = [2021, 2021, 2021, 2022]
        dose_months = [0, 1, 12, 1]

        base_df = self.__create_base_df(
            {
                OzpBaseColumn.VACCINE_1_YEAR: [dose_years[0]],
                OzpBaseColumn.VACCINE_1_MONTH: [dose_months[0]],
                OzpBaseColumn.VACCINE_2_YEAR: [dose_years[1]],
                OzpBaseColumn.VACCINE_2_MONTH: [dose_months[1]],
                OzpBaseColumn.VACCINE_3_YEAR: [dose_years[2]],
                OzpBaseColumn.VACCINE_3_MONTH: [dose_months[2]],
                OzpBaseColumn.VACCINE_4_YEAR: [dose_years[3]],
                OzpBaseColumn.VACCINE_4_MONTH: [dose_months[3]],
            }
        )

        result = self.__generator.generate(base_df)

        for idx, dose_month in enumerate(dose_months):
            expected_index = self.__generator.month_indices.get(f"{dose_years[idx]}M{str(dose_month).zfill(2)}")
            dose_series = self.__get_column_for_person(
                df=result,
                person_id=1,
                col_name=f"dose{idx+1}",  # +1 because dose 0 does not exist
            )
            self.assertTrue(all(val == expected_index for val in dose_series))

    # - When generating the template from a base DataFrame
    # -- It should correctly convert the death month to a numeric index (death_index)
    def test_death_index_conversion(self):
        base_df = self.__create_base_df({OzpBaseColumn.DEATH_YEAR: [2021], OzpBaseColumn.DEATH_MONTH: [5]})
        result = self.__generator.generate(base_df)

        expected_death_index = self.__generator.month_indices.get("2021M05")
        death_index_series = result.filter(col(TempColumn.PERSON_ID) == 1).select(TempColumn.DEATH_INDEX).to_series()
        self.assertTrue(all(val == expected_death_index for val in death_index_series))

    # - When generating the template from a base DataFrame
    # -- And the person died before the requested time span
    # --- It should set the death index to -1
    def test_death_index_before_requested_time_span(self):
        base_df = self.__create_base_df({OzpBaseColumn.DEATH_YEAR: [2020], OzpBaseColumn.DEATH_MONTH: [1]})
        result_df = self.__generator.generate(base_df)

        person_death_indices = self.__get_column_for_person(
            df=result_df,
            person_id=1,
            col_name=TempColumn.DEATH_INDEX,
        )
        self.assertTrue(all(val == -1 for val in person_death_indices))

    # - When generating the template from a base DataFrame
    # -- And the person died after the requested time span
    # --- It should set the death index to maximum value
    def test_death_index_after_requested_time_span(self):
        base_df = self.__create_base_df({OzpBaseColumn.DEATH_YEAR: [2023], OzpBaseColumn.DEATH_MONTH: [1]})
        result_df = self.__generator.generate(base_df)

        person_death_indices = self.__get_column_for_person(
            df=result_df,
            person_id=1,
            col_name=TempColumn.DEATH_INDEX,
        )
        self.assertTrue(all(val == MAX_TIME_PERIOD_VALUE for val in person_death_indices))

    # - When generating the template from a base DataFrame
    # -- And the person did not die at all
    # --- It should set the death index to maximum value
    def test_death_index_not_set(self):
        base_df = self.__create_base_df()
        result_df = self.__generator.generate(base_df)

        person_death_indices = self.__get_column_for_person(
            df=result_df,
            person_id=1,
            col_name=TempColumn.DEATH_INDEX,
        )
        self.assertTrue(all(val == MAX_TIME_PERIOD_VALUE for val in person_death_indices))

    def __get_column_for_person(self, df: DataFrame, person_id: int, col_name: str) -> Series:
        return df.filter(col(TempColumn.PERSON_ID) == person_id).select(col_name).to_series()

    def __create_base_df(
        self,
        overrides: Optional[dict[OzpBaseColumn, list[str | int]]] = None,
        num_of_rows: int = 1,
    ) -> DataFrame:
        if overrides is None:
            overrides = {}

        if len(overrides):
            num_of_rows = max(len(v) for v in overrides.values())

        base_data = {col: ["" if dtype == Utf8 else None] * num_of_rows for col, dtype in OzpBaseDfSchema.items()}

        for key, values in overrides.items():
            base_data[key.value] = values  # type: ignore

        return DataFrame(base_data, schema=OzpBaseDfSchema)
