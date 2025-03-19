from dataclasses import dataclass

from polars import DataFrame, col

from common.typings import AgeStatus, AliveStatus, NewColumn, VaccineStatus


@dataclass(frozen=True)
class ACMResult:
    acm: float
    total_deaths: int


class ACMCalculator:
    def compute_person_years_acm(
        self,
        df: DataFrame,
        age_status: AgeStatus,
        vac_status: VaccineStatus,
        is_using_months: bool,
    ) -> ACMResult:
        alive_person_weeks = df.filter(
            (col(NewColumn.DEATH_STATUS) == AliveStatus.ALIVE)
            & (col(NewColumn.AGE) == age_status)
            & (col(NewColumn.VACCINE_STATUS) == vac_status)
        ).shape[0]

        total_deaths = df.filter(
            (col(NewColumn.DEATH_STATUS) == AliveStatus.DIED_NOW)
            & (col(NewColumn.AGE) == age_status)
            & (col(NewColumn.VACCINE_STATUS) == vac_status)
        ).shape[0]

        if alive_person_weeks == 0:
            return ACMResult(acm=0, total_deaths=total_deaths)

        num_of_time_periods_in_year = 12 if is_using_months else 52
        acm = (total_deaths / alive_person_weeks) * 100_000 * num_of_time_periods_in_year

        return ACMResult(acm=acm, total_deaths=total_deaths)
