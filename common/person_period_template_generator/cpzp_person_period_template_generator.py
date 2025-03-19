from datetime import datetime
from enum import StrEnum

from polars import DataFrame, Expr, Int64, Schema, Utf8, col, when

from common.person_period_template_generator.person_period_template_generator import PersonPeriodTemplateGenerator, TempColumn
from common.time_period.time_period_helper import TimePeriodHelper
from common.typings import MAX_TIME_PERIOD_VALUE


class CpzpBaseColumn(StrEnum):
    BIRTHDATE = "tyden_narozeni"
    GENDER = "POHLAVI"
    VACCINE_1_DATE = "vakcina1_tyden"
    VACCINE_1_CODE = "vakcina1_kod"
    VACCINE_2_DATE = "vakcina2_tyden"
    VACCINE_2_CODE = "vakcina2_kod"
    VACCINE_3_DATE = "vakcina3_tyden"
    VACCINE_3_CODE = "vakcina3_kod"
    VACCINE_4_DATE = "vakcina4_tyden"
    VACCINE_4_CODE = "vakcina4_kod"
    VACCINE_5_DATE = "vakcina5_tyden"
    VACCINE_5_CODE = "vakcina5_kod"
    DEATHDATE = "tyden_umrti"


CpzpBaseDfSchema = Schema(
    {
        CpzpBaseColumn.BIRTHDATE.value: Utf8,
        CpzpBaseColumn.GENDER.value: Utf8,
        CpzpBaseColumn.VACCINE_1_DATE.value: Utf8,
        CpzpBaseColumn.VACCINE_1_CODE.value: Utf8,
        CpzpBaseColumn.VACCINE_2_DATE.value: Utf8,
        CpzpBaseColumn.VACCINE_2_CODE.value: Utf8,
        CpzpBaseColumn.VACCINE_3_DATE.value: Utf8,
        CpzpBaseColumn.VACCINE_3_CODE.value: Utf8,
        CpzpBaseColumn.VACCINE_4_DATE.value: Utf8,
        CpzpBaseColumn.VACCINE_4_CODE.value: Utf8,
        CpzpBaseColumn.VACCINE_5_DATE.value: Utf8,
        CpzpBaseColumn.VACCINE_5_CODE.value: Utf8,
        CpzpBaseColumn.DEATHDATE.value: Utf8,
    }
)


class CpzpPersonPeriodTemplateGenerator(PersonPeriodTemplateGenerator):
    def __init__(self, start_date: datetime, end_date: datetime) -> None:
        self.weeks = TimePeriodHelper.get_weeks_in_range(start_date, end_date)
        self.week_indices = {week: i for i, week in enumerate(self.weeks)}

    def generate(self, base_df: DataFrame) -> DataFrame:
        self.__assert_schema_matches_cpzp(base_df)

        persons_df = (
            base_df.with_row_index(TempColumn.PERSON_ID, offset=1)
            .with_columns(
                [
                    self.__map_iso_week_to_week_index(CpzpBaseColumn.VACCINE_1_DATE).alias(TempColumn.DOSE_1),
                    self.__map_iso_week_to_week_index(CpzpBaseColumn.VACCINE_2_DATE).alias(TempColumn.DOSE_2),
                    self.__map_iso_week_to_week_index(CpzpBaseColumn.VACCINE_3_DATE).alias(TempColumn.DOSE_3),
                    self.__map_iso_week_to_week_index(CpzpBaseColumn.VACCINE_4_DATE).alias(TempColumn.DOSE_4),
                    col(CpzpBaseColumn.DEATHDATE)
                    .map_elements(self.__compute_death_index, return_dtype=Int64)
                    .alias(TempColumn.DEATH_INDEX),
                ]
            )
            .select(
                TempColumn.PERSON_ID,
                TempColumn.BIRTHDATE,
                TempColumn.DOSE_1,
                TempColumn.DOSE_2,
                TempColumn.DOSE_3,
                TempColumn.DOSE_4,
                TempColumn.DEATH_INDEX,
            )
        )

        weeks_df = DataFrame(
            {
                TempColumn.TIME_PERIOD: self.weeks,
                TempColumn.TIME_PERIOD_INDEX: list(range(len(self.weeks))),
            }
        )

        return persons_df.join(weeks_df, how="cross")

    def __map_iso_week_to_week_index(self, col_name: str) -> Expr:
        return (
            when(col(col_name).str.strip_chars().is_in(["", " "]))
            .then(None)
            .otherwise(col(col_name))
            .replace(self.week_indices)
            .cast(Int64, strict=False)
        )

    def __compute_death_index(self, death_week: str) -> int:
        if not death_week or death_week.strip() in {"", " "}:
            return MAX_TIME_PERIOD_VALUE

        current_year = int(death_week[:4])
        min_year = int(self.weeks[0][:4])
        max_year = int(self.weeks[-1][:4])

        if current_year < min_year:
            return -1

        if current_year > max_year:
            return MAX_TIME_PERIOD_VALUE

        return self.week_indices.get(death_week, -1)

    def __assert_schema_matches_cpzp(self, df: DataFrame) -> None:
        assert df.schema == CpzpBaseDfSchema, "CPZP dataframe does not follow the expected CPZP schema"
