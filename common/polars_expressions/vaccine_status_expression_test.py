from typing import Optional
from unittest import TestCase

from polars import DataFrame

from common.person_period_template_generator.person_period_template_generator import TempColumn
from common.polars_expressions.vaccine_status_expression import VaccineStatusExpression
from common.typings import VaccineStatus


class TestVaccineStatusExpression(TestCase):
    # - When using the vaccine status expression
    # -- And the person is unvaccinated (no dose data)
    # --- It should classify the status as UNVACCINATED
    def test_unvaccinated(self) -> None:
        df = self.__create_base_df()
        result = df.with_columns(VaccineStatusExpression.get_expr(4).alias("vaccine_status"))
        self.assertEqual(result["vaccine_status"].to_list(), [VaccineStatus.UNVACCINATED.value])

    # - When using the vaccine status expression
    # -- And the time_period_index is less than configured amount of time_periods after dose1
    # --- It should classify the status as LESS_THAN_4_WEEKS_FROM_DOSE_1
    def test_less_than_4_weeks_from_dose_1(self) -> None:
        df = self.__create_base_df({TempColumn.TIME_PERIOD_INDEX: [10], TempColumn.DOSE_1: [7]})
        result = df.with_columns(VaccineStatusExpression.get_expr(4).alias("vaccine_status"))
        self.assertEqual(
            result["vaccine_status"].to_list(),
            [VaccineStatus.LESS_THAN_4_WEEKS_FROM_DOSE_1.value],
        )

    # - When using the vaccine status expression
    # -- And the time_period_index is more than configured amount of time_periods after dose1
    # --- It should classify the status as MORE_THAN_4_WEEKS_FROM_DOSE_2
    def test_more_than_4_weeks_from_dose_1(self) -> None:
        df = self.__create_base_df({TempColumn.TIME_PERIOD_INDEX: [10], TempColumn.DOSE_1: [6]})
        result = df.with_columns(VaccineStatusExpression.get_expr(4).alias("vaccine_status"))
        self.assertEqual(
            result["vaccine_status"].to_list(),
            [VaccineStatus.MORE_THAN_4_WEEKS_FROM_DOSE_1.value],
        )

    # - When using the vaccine status expression
    # -- And the time_period_index is less than configured amount of time_periods after dose2
    # --- It should classify the status as LESS_THAN_4_WEEKS_FROM_DOSE_2
    def test_less_than_4_weeks_from_dose_2(self) -> None:
        df = self.__create_base_df({TempColumn.TIME_PERIOD_INDEX: [10], TempColumn.DOSE_2: [7]})
        result = df.with_columns(VaccineStatusExpression.get_expr(4).alias("vaccine_status"))
        self.assertEqual(
            result["vaccine_status"].to_list(),
            [VaccineStatus.LESS_THAN_4_WEEKS_FROM_DOSE_2.value],
        )

    # - When using the vaccine status expression
    # -- And the time_period_index is more than configured amount of time_periods after dose2
    # --- It should classify the status as MORE_THAN_4_WEEKS_FROM_DOSE_2
    def test_more_than_4_weeks_from_dose_2(self) -> None:
        df = self.__create_base_df({TempColumn.TIME_PERIOD_INDEX: [10], TempColumn.DOSE_2: [6]})
        result = df.with_columns(VaccineStatusExpression.get_expr(4).alias("vaccine_status"))
        self.assertEqual(
            result["vaccine_status"].to_list(),
            [VaccineStatus.MORE_THAN_4_WEEKS_FROM_DOSE_2.value],
        )

    # - When using the vaccine status expression
    # -- And the time_period_index is less than configured amount of time_periods after dose3
    # --- It should classify the status as LESS_THAN_4_WEEKS_FROM_DOSE_3
    def test_less_than_4_weeks_from_dose_3(self) -> None:
        df = self.__create_base_df({TempColumn.TIME_PERIOD_INDEX: [10], TempColumn.DOSE_3: [7]})
        result = df.with_columns(VaccineStatusExpression.get_expr(4).alias("vaccine_status"))
        self.assertEqual(
            result["vaccine_status"].to_list(),
            [VaccineStatus.LESS_THAN_4_WEEKS_FROM_DOSE_3.value],
        )

    # - When using the vaccine status expression
    # -- And the time_period_index is more than configured amount of time_periods after dose3
    # --- It should classify the status as MORE_THAN_4_WEEKS_FROM_DOSE_3
    def test_more_than_4_weeks_from_dose_3(self) -> None:
        df = self.__create_base_df({TempColumn.TIME_PERIOD_INDEX: [10], TempColumn.DOSE_3: [6]})
        result = df.with_columns(VaccineStatusExpression.get_expr(4).alias("vaccine_status"))
        self.assertEqual(
            result["vaccine_status"].to_list(),
            [VaccineStatus.MORE_THAN_4_WEEKS_FROM_DOSE_3.value],
        )

    # - When using the vaccine status expression
    # -- And the time_period_index is less than configured amount of time_periods after dose4
    # --- It should classify the status as DOSE_4_OR_HIGHER
    def test_less_than_4_weeks_from_dose_4(self) -> None:
        df = self.__create_base_df({TempColumn.TIME_PERIOD_INDEX: [10], TempColumn.DOSE_4: [7]})
        result = df.with_columns(VaccineStatusExpression.get_expr(4).alias("vaccine_status"))
        self.assertEqual(
            result["vaccine_status"].to_list(),
            [VaccineStatus.DOSE_4_OR_HIGHER.value],
        )

    # - When using the vaccine status expression
    # -- And the time_period_index is more than configured amount of time_periods after dose4
    # --- It should classify the status as DOSE_4_OR_HIGHER
    def test_more_than_4_weeks_from_dose_4(self) -> None:
        df = self.__create_base_df({TempColumn.TIME_PERIOD_INDEX: [10], TempColumn.DOSE_4: [6]})
        result = df.with_columns(VaccineStatusExpression.get_expr(4).alias("vaccine_status"))
        self.assertEqual(
            result["vaccine_status"].to_list(),
            [VaccineStatus.DOSE_4_OR_HIGHER.value],
        )

    # - Whehn using the vaccine status expression
    # -- It should correctly classify the status for possible combinations
    def test_all_combinations(self) -> None:
        df = self.__create_base_df(
            {
                TempColumn.TIME_PERIOD_INDEX: list(range(1, 21)),
                TempColumn.DOSE_1: [2 for _ in range(20)],
                TempColumn.DOSE_2: [8 for _ in range(20)],
                TempColumn.DOSE_3: [14 for _ in range(20)],
                TempColumn.DOSE_4: [20 for _ in range(20)],
            }
        )
        result = df.with_columns(VaccineStatusExpression.get_expr(4).alias("vaccine_status"))
        expected_vaccine_status = [
            VaccineStatus.UNVACCINATED.value,  # First week is unvaccinated
            *[VaccineStatus.LESS_THAN_4_WEEKS_FROM_DOSE_1.value] * 4,  # Next 4 weeks are less than 4 weeks from dose 1
            *[VaccineStatus.MORE_THAN_4_WEEKS_FROM_DOSE_1.value] * 2,  # Next 2 weeks are more than 4 weeks from dose 1
            *[VaccineStatus.LESS_THAN_4_WEEKS_FROM_DOSE_2.value] * 4,  # ...
            *[VaccineStatus.MORE_THAN_4_WEEKS_FROM_DOSE_2.value] * 2,
            *[VaccineStatus.LESS_THAN_4_WEEKS_FROM_DOSE_3.value] * 4,
            *[VaccineStatus.MORE_THAN_4_WEEKS_FROM_DOSE_3.value] * 2,
            VaccineStatus.DOSE_4_OR_HIGHER.value,
        ]

        self.assertEqual(result["vaccine_status"].to_list(), expected_vaccine_status)

    def __create_base_df(
        self,
        overrides: Optional[dict[TempColumn, list[int]]] = None,
        num_of_rows: int = 1,
    ) -> DataFrame:
        if overrides is None:
            overrides = {}

        if len(overrides):
            num_of_rows = max(len(v) for v in overrides.values())

        base_data = {col.value: [None] * num_of_rows for col in TempColumn}

        for key, values in overrides.items():
            base_data[key.value] = values  # type: ignore

        return DataFrame(base_data)
