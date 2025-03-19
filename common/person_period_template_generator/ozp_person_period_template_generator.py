from datetime import datetime
from enum import StrEnum

from polars import DataFrame, Expr, Int64, Schema, Utf8, col, when

from common.person_period_template_generator.person_period_template_generator import PersonPeriodTemplateGenerator, TempColumn
from common.time_period.time_period_helper import TimePeriodHelper
from common.typings import MAX_TIME_PERIOD_VALUE


class OzpBaseColumn(StrEnum):
    ID_POJ = "Id poj."
    GENDER = "Pohlaví"
    BIRTH_YEAR = "Rok Narození"
    BIRTH_MONTH = "Měsíc Narození"
    DEATH_YEAR = "Rok Úmrtí"
    DEATH_MONTH = "Měsíc Úmrtí"

    VACCINE_1_CODE = "Kód Vakcína 1"
    VACCINE_1_YEAR = "Rok Vakcína 1"
    VACCINE_1_MONTH = "Měsíc Vakcína 1"

    VACCINE_2_CODE = "Kód Vakcína 2"
    VACCINE_2_YEAR = "Rok Vakcína 2"
    VACCINE_2_MONTH = "Měsíc Vakcína 2"

    VACCINE_3_CODE = "Kód Vakcína 3"
    VACCINE_3_YEAR = "Rok Vakcína 3"
    VACCINE_3_MONTH = "Měsíc Vakcína 3"

    VACCINE_4_CODE = "Kód Vakcína 4"
    VACCINE_4_YEAR = "Rok Vakcína 4"
    VACCINE_4_MONTH = "Měsíc Vakcína 4"

    VACCINE_5_CODE = "Kód Vakcína 5"
    VACCINE_5_YEAR = "Rok Vakcína 5"
    VACCINE_5_MONTH = "Měsíc Vakcína 5"

    VACCINE_6_CODE = "Kód Vakcína 6"
    VACCINE_6_YEAR = "Rok Vakcína 6"
    VACCINE_6_MONTH = "Měsíc Vakcína 6"

    VACCINE_7_CODE = "Kód Vakcína 7"
    VACCINE_7_YEAR = "Rok Vakcína 7"
    VACCINE_7_MONTH = "Měsíc Vakcína 7"


OzpBaseDfSchema = Schema(
    {
        OzpBaseColumn.ID_POJ.value: Int64,
        OzpBaseColumn.GENDER.value: Int64,
        OzpBaseColumn.BIRTH_YEAR.value: Int64,
        OzpBaseColumn.BIRTH_MONTH.value: Int64,
        OzpBaseColumn.DEATH_YEAR.value: Int64,
        OzpBaseColumn.DEATH_MONTH.value: Int64,
        OzpBaseColumn.VACCINE_1_CODE.value: Int64,
        OzpBaseColumn.VACCINE_1_YEAR.value: Int64,
        OzpBaseColumn.VACCINE_1_MONTH.value: Int64,
        OzpBaseColumn.VACCINE_2_CODE.value: Int64,
        OzpBaseColumn.VACCINE_2_YEAR.value: Int64,
        OzpBaseColumn.VACCINE_2_MONTH.value: Int64,
        OzpBaseColumn.VACCINE_3_CODE.value: Int64,
        OzpBaseColumn.VACCINE_3_YEAR.value: Int64,
        OzpBaseColumn.VACCINE_3_MONTH.value: Int64,
        OzpBaseColumn.VACCINE_4_CODE.value: Int64,
        OzpBaseColumn.VACCINE_4_YEAR.value: Int64,
        OzpBaseColumn.VACCINE_4_MONTH.value: Int64,
        OzpBaseColumn.VACCINE_5_CODE.value: Utf8,
        OzpBaseColumn.VACCINE_5_YEAR.value: Utf8,
        OzpBaseColumn.VACCINE_5_MONTH.value: Utf8,
        OzpBaseColumn.VACCINE_6_CODE.value: Utf8,
        OzpBaseColumn.VACCINE_6_YEAR.value: Utf8,
        OzpBaseColumn.VACCINE_6_MONTH.value: Utf8,
        OzpBaseColumn.VACCINE_7_CODE.value: Utf8,
        OzpBaseColumn.VACCINE_7_YEAR.value: Utf8,
        OzpBaseColumn.VACCINE_7_MONTH.value: Utf8,
    }
)


class OzpPersonPeriodTemplateGenerator(PersonPeriodTemplateGenerator):
    def __init__(self, start_date: datetime, end_date: datetime) -> None:
        self.months = TimePeriodHelper.get_months_in_range(start_date, end_date)
        self.month_indices = {month: i for i, month in enumerate(self.months)}

    def generate(self, base_df: DataFrame) -> DataFrame:
        self.__assert_schema_matches_ozp(base_df)

        persons_df = (
            base_df.with_row_index(TempColumn.PERSON_ID, offset=1)
            .with_columns(
                [
                    # Compute Vaccine doses indexes
                    self.__map_iso_month_to_month_index(OzpBaseColumn.VACCINE_1_YEAR, OzpBaseColumn.VACCINE_1_MONTH).alias(
                        TempColumn.DOSE_1
                    ),
                    self.__map_iso_month_to_month_index(OzpBaseColumn.VACCINE_2_YEAR, OzpBaseColumn.VACCINE_2_MONTH).alias(
                        TempColumn.DOSE_2
                    ),
                    self.__map_iso_month_to_month_index(OzpBaseColumn.VACCINE_3_YEAR, OzpBaseColumn.VACCINE_3_MONTH).alias(
                        TempColumn.DOSE_3
                    ),
                    self.__map_iso_month_to_month_index(OzpBaseColumn.VACCINE_4_YEAR, OzpBaseColumn.VACCINE_4_MONTH).alias(
                        TempColumn.DOSE_4
                    ),
                    # Compute death indexes
                    when((col(OzpBaseColumn.DEATH_YEAR).is_null()) | (col(OzpBaseColumn.DEATH_MONTH).is_null()))
                    .then(MAX_TIME_PERIOD_VALUE)
                    .otherwise(
                        self.__join_year_and_month(OzpBaseColumn.DEATH_YEAR, OzpBaseColumn.DEATH_MONTH).map_elements(
                            self.__compute_death_index, return_dtype=Int64
                        )
                    )
                    .alias(TempColumn.DEATH_INDEX),
                    # Compute birthdate
                    self.__join_year_and_month(OzpBaseColumn.BIRTH_YEAR, OzpBaseColumn.BIRTH_MONTH).alias(TempColumn.BIRTHDATE),
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
                TempColumn.TIME_PERIOD: self.months,
                TempColumn.TIME_PERIOD_INDEX: list(range(len(self.months))),
            }
        )

        return persons_df.join(weeks_df, how="cross")

    def __map_iso_month_to_month_index(self, year_col: str, month_col: str) -> Expr:
        return (
            when((col(year_col).is_null()) | (col(month_col).is_null()))
            .then(None)
            .otherwise(self.__join_year_and_month(year_col, month_col))
            .replace(self.month_indices)
            .cast(Int64, strict=False)
        )

    def __compute_death_index(self, death_week: str) -> int:
        if not death_week or death_week.strip() in {"", " "}:
            return MAX_TIME_PERIOD_VALUE

        current_year = int(death_week[:4])
        min_year = int(self.months[0][:4])
        max_year = int(self.months[-1][:4])

        if current_year < min_year:
            return -1

        if current_year > max_year:
            return MAX_TIME_PERIOD_VALUE

        return self.month_indices.get(death_week, -1)

    def __join_year_and_month(self, year_col: str, month_col: str) -> Expr:
        return col(year_col).cast(Utf8) + col(month_col).map_elements(lambda x: f"M{str(x).zfill(2)}", return_dtype=Utf8)

    def __assert_schema_matches_ozp(self, df: DataFrame) -> None:
        assert df.schema == OzpBaseDfSchema, "OZP dataframe does not follow the expected OZP schema"
