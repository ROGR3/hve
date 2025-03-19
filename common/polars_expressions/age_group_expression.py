from polars import Expr, Int64, col, lit, when

from common.person_period_template_generator.person_period_template_generator import TempColumn
from common.typings import AgeStatus


class AgeGroupExpression:
    @staticmethod
    def get_expr() -> Expr:
        age_expr = (
            col(TempColumn.TIME_PERIOD).str.slice(0, 4).cast(Int64)
            - col(TempColumn.BIRTHDATE).str.slice(0, 4).cast(Int64)
            - (
                (
                    col(TempColumn.TIME_PERIOD).str.slice(5, 2).cast(Int64) < col(TempColumn.BIRTHDATE).str.slice(5, 2).cast(Int64)
                ).cast(Int64)
            )
        )
        return (
            when(age_expr < 60)
            .then(lit(AgeStatus.LESS_THAN_60))
            .when(age_expr < 70)
            .then(lit(AgeStatus.BETWEEN_60_AND_69))
            .when(age_expr < 80)
            .then(lit(AgeStatus.BETWEEN_70_AND_79))
            .otherwise(lit(AgeStatus.GREATER_THAN_80))
        )
