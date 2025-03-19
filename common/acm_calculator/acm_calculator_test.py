from unittest import TestCase

from polars import DataFrame

from common.acm_calculator.acm_calculator import ACMCalculator
from common.typings import AgeStatus, AliveStatus, NewColumn, VaccineStatus


class TestACMCalculator(TestCase):
    def setUp(self) -> None:
        self.acm_calculator = ACMCalculator()

    # - Given a DataFrame with no alive persons
    # -- When calling compute_person_years_acm method
    # -- It should return 0 ACM and the total number of deaths
    def test_compute_person_years_acm_should_return_zero_acm_when_no_alive_persons(self):
        df = DataFrame(
            {
                NewColumn.DEATH_STATUS: [AliveStatus.DIED_NOW, AliveStatus.DIED_NOW],
                NewColumn.AGE: [AgeStatus.LESS_THAN_60, AgeStatus.LESS_THAN_60],
                NewColumn.VACCINE_STATUS: [VaccineStatus.UNVACCINATED, VaccineStatus.UNVACCINATED],
            }
        )

        result = self.acm_calculator.compute_person_years_acm(
            df, AgeStatus.LESS_THAN_60, VaccineStatus.UNVACCINATED, is_using_months=False
        )

        self.assertEqual(result.acm, 0)
        self.assertEqual(result.total_deaths, 2)

    # - Given a DataFrame with both alive and dead persons
    # -- When calling compute_person_years_acm method with is_using_months set to False
    # -- It should correctly calculate the ACM based on the deaths and alive persons
    def test_compute_person_years_acm_should_return_correct_acm_for_alive_and_dead_persons(self):
        df = DataFrame(
            {
                NewColumn.DEATH_STATUS: [AliveStatus.ALIVE, AliveStatus.ALIVE, AliveStatus.DIED_NOW, AliveStatus.DIED_NOW],
                NewColumn.AGE: [AgeStatus.LESS_THAN_60, AgeStatus.LESS_THAN_60, AgeStatus.LESS_THAN_60, AgeStatus.LESS_THAN_60],
                NewColumn.VACCINE_STATUS: [
                    VaccineStatus.UNVACCINATED,
                    VaccineStatus.UNVACCINATED,
                    VaccineStatus.UNVACCINATED,
                    VaccineStatus.UNVACCINATED,
                ],
            }
        )

        result = self.acm_calculator.compute_person_years_acm(
            df, AgeStatus.LESS_THAN_60, VaccineStatus.UNVACCINATED, is_using_months=False
        )

        expected_acm = (2 / 2) * 100_000 * 52  # (2 deaths, 2 alive person-periods, 52 time periods per year)
        self.assertEqual(result.acm, expected_acm)
        self.assertEqual(result.total_deaths, 2)

    # - Given a DataFrame with both alive and dead persons
    # -- When calling compute_person_years_acm method with is_using_months set to True
    # -- It should calculate ACM using months instead of weeks (12 months in a year)
    def test_compute_person_years_acm_should_use_months_when_is_using_months_is_true(self):
        df = DataFrame(
            {
                NewColumn.DEATH_STATUS: [AliveStatus.ALIVE, AliveStatus.ALIVE, AliveStatus.DIED_NOW, AliveStatus.DIED_NOW],
                NewColumn.AGE: [AgeStatus.LESS_THAN_60, AgeStatus.LESS_THAN_60, AgeStatus.LESS_THAN_60, AgeStatus.LESS_THAN_60],
                NewColumn.VACCINE_STATUS: [
                    VaccineStatus.UNVACCINATED,
                    VaccineStatus.UNVACCINATED,
                    VaccineStatus.UNVACCINATED,
                    VaccineStatus.UNVACCINATED,
                ],
            }
        )

        result = self.acm_calculator.compute_person_years_acm(
            df, AgeStatus.LESS_THAN_60, VaccineStatus.UNVACCINATED, is_using_months=True
        )

        expected_acm = (2 / 2) * 100_000 * 12  # (2 deaths, 2 alive person-periods, 12 time periods per year)
        self.assertEqual(result.acm, expected_acm)
        self.assertEqual(result.total_deaths, 2)

    # - Given a DataFrame with no deaths
    # -- When calling compute_person_years_acm method
    # -- It should return 0 ACM and 0 deaths
    def test_compute_person_years_acm_should_return_zero_when_no_deaths(self):
        df = DataFrame(
            {
                NewColumn.DEATH_STATUS: [AliveStatus.ALIVE, AliveStatus.ALIVE],
                NewColumn.AGE: [AgeStatus.LESS_THAN_60, AgeStatus.LESS_THAN_60],
                NewColumn.VACCINE_STATUS: [VaccineStatus.UNVACCINATED, VaccineStatus.UNVACCINATED],
            }
        )

        result = self.acm_calculator.compute_person_years_acm(
            df, AgeStatus.LESS_THAN_60, VaccineStatus.UNVACCINATED, is_using_months=False
        )

        self.assertEqual(result.acm, 0)
        self.assertEqual(result.total_deaths, 0)

    # - Given an empty DataFrame
    # -- When calling compute_person_years_acm method
    # -- It should return 0 ACM and 0 deaths
    def test_compute_person_years_acm_should_handle_empty_dataframe(self):
        df = DataFrame({NewColumn.DEATH_STATUS: [], NewColumn.AGE: [], NewColumn.VACCINE_STATUS: []})

        result = self.acm_calculator.compute_person_years_acm(
            df, AgeStatus.LESS_THAN_60, VaccineStatus.UNVACCINATED, is_using_months=False
        )

        self.assertEqual(result.acm, 0)
        self.assertEqual(result.total_deaths, 0)
