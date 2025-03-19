from unittest import TestCase

from polars import DataFrame

from common.person_period_template_generator.person_period_template_generator import TempColumn
from common.polars_expressions.death_status_expression import DeathStatusExpression
from common.typings import AliveStatus


class TestDeathStatusExpression(TestCase):
    # - When using the death status expression
    # -- And week_index is greater than death_index (eg. the person died earlier)
    # --- It should classify the status as AFTER_DEATH
    def test_after_death(self):
        df = DataFrame({TempColumn.TIME_PERIOD_INDEX: [10], TempColumn.DEATH_INDEX: [5]})
        result = df.with_columns(DeathStatusExpression.get_expr().alias("death_status"))
        self.assertEqual(result["death_status"].to_list(), [AliveStatus.AFTER_DEATH])

    # - When using the death status expression
    # -- And week_index equals the death_index (eg. the person died this week)
    # --- It should classify the status as DIED_NOW
    def test_died_now(self):
        df = DataFrame({TempColumn.TIME_PERIOD_INDEX: [10], TempColumn.DEATH_INDEX: [10]})
        result = df.with_columns(DeathStatusExpression.get_expr().alias("death_status"))
        self.assertEqual(result["death_status"].to_list(), [AliveStatus.DIED_NOW])

    # - When using the death status expression
    # -- And week_index is less than death_index (eg. the person is alive)
    # --- It should classify the status as ALIVE
    def test_alive(self):
        df = DataFrame({TempColumn.TIME_PERIOD_INDEX: [5], TempColumn.DEATH_INDEX: [10]})
        result = df.with_columns(DeathStatusExpression.get_expr().alias("death_status"))
        self.assertEqual(result["death_status"].to_list(), [AliveStatus.ALIVE])

    # - When using the death status expression
    # -- And multiple rows with varying conditions are provided
    # --- It should correctly classify the status each row
    def test_multiple_rows(self):
        df = DataFrame(
            {
                TempColumn.TIME_PERIOD_INDEX: [10, 10, 5],
                TempColumn.DEATH_INDEX: [5, 10, 10],
            }
        )
        result = df.with_columns(DeathStatusExpression.get_expr().alias("death_status"))
        expected = [
            AliveStatus.AFTER_DEATH,  # 10 > 5
            AliveStatus.DIED_NOW,  # 10 == 10
            AliveStatus.ALIVE,  # 5 < 10
        ]
        self.assertEqual(result["death_status"].to_list(), expected)
