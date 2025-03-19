from polars import Expr, col, lit, when

from common.person_period_template_generator.person_period_template_generator import TempColumn
from common.typings import AliveStatus


class DeathStatusExpression:
    @staticmethod
    def get_expr() -> Expr:
        return (
            when(col(TempColumn.TIME_PERIOD_INDEX) > col(TempColumn.DEATH_INDEX))
            .then(lit(AliveStatus.AFTER_DEATH))
            .otherwise(
                when(col(TempColumn.TIME_PERIOD_INDEX) == col(TempColumn.DEATH_INDEX))
                .then(lit(AliveStatus.DIED_NOW))
                .otherwise(lit(AliveStatus.ALIVE))
            )
        )
