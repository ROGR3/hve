from enum import StrEnum
from typing import Protocol

from polars import DataFrame, Int64, Schema, UInt32, Utf8


class TempColumn(StrEnum):
    PERSON_ID = "person_id"
    TIME_PERIOD = "time_period"
    TIME_PERIOD_INDEX = "time_period_index"
    DOSE_1 = "dose1"
    DOSE_2 = "dose2"
    DOSE_3 = "dose3"
    DOSE_4 = "dose4"
    DEATH_INDEX = "death_index"
    BIRTHDATE = "tyden_narozeni"


PersonPeriodDfSchema = Schema(
    {
        TempColumn.PERSON_ID.value: UInt32,
        TempColumn.BIRTHDATE.value: Utf8,
        TempColumn.DOSE_1.value: Int64,
        TempColumn.DOSE_2.value: Int64,
        TempColumn.DOSE_3.value: Int64,
        TempColumn.DOSE_4.value: Int64,
        TempColumn.DEATH_INDEX.value: Int64,
        TempColumn.TIME_PERIOD.value: Utf8,
        TempColumn.TIME_PERIOD_INDEX.value: Int64,
    }
)


class PersonPeriodTemplateGenerator(Protocol):
    def __init__(self, start_year: int, end_year: int) -> None: ...

    def generate(self, base_df: DataFrame) -> DataFrame: ...
