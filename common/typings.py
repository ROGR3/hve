from enum import Enum, StrEnum


class AliveStatus(StrEnum):
    ALIVE = "alive"
    DIED_NOW = "died_now"
    AFTER_DEATH = "after_death"


class AgeStatus(StrEnum):
    LESS_THAN_60 = "<60"
    BETWEEN_60_AND_69 = "60-69"
    BETWEEN_70_AND_79 = "70-79"
    GREATER_THAN_80 = "80+"


class AnalysedAgeStatus(StrEnum):
    BETWEEN_60_AND_69 = "60-69"
    BETWEEN_70_AND_79 = "70-79"
    GREATER_THAN_80 = "80+"


class VaccineStatus(Enum):
    UNVACCINATED = 0
    LESS_THAN_4_WEEKS_FROM_DOSE_1 = 10
    MORE_THAN_4_WEEKS_FROM_DOSE_1 = 11
    LESS_THAN_4_WEEKS_FROM_DOSE_2 = 20
    MORE_THAN_4_WEEKS_FROM_DOSE_2 = 21
    LESS_THAN_4_WEEKS_FROM_DOSE_3 = 30
    MORE_THAN_4_WEEKS_FROM_DOSE_3 = 31
    DOSE_4_OR_HIGHER = 40


class NewColumn(StrEnum):
    PERSON_ID = "person_id"
    TIME_PERIOD = "time_period"
    AGE = "age"
    DEATH_STATUS = "death_status"
    VACCINE_STATUS = "vaccine_status"


MAX_TIME_PERIOD_VALUE = 1_000  # there cant be more than 1000 time periods (weeks/years)


COLORS = [
    "#19004D",
    "#0033FF",
    "#66B2FF",
    "#99FF99",
    "#FFFF66",
    "#FFA500",
    "#FF0000",
    "#800000",
]
