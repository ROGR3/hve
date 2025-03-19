from polars import Expr, Int64, col, lit, when

from common.person_period_template_generator.person_period_template_generator import TempColumn
from common.typings import VaccineStatus


class VaccineStatusExpression:
    @staticmethod
    def get_expr(time_period_amount: int) -> Expr:
        return (
            when(col(TempColumn.DOSE_4).is_not_null() & (col(TempColumn.TIME_PERIOD_INDEX) >= col(TempColumn.DOSE_4).cast(Int64)))
            .then(lit(VaccineStatus.DOSE_4_OR_HIGHER))
            .otherwise(
                when((col(TempColumn.DOSE_3).is_not_null()) & (col(TempColumn.TIME_PERIOD_INDEX) >= col(TempColumn.DOSE_3)))
                .then(
                    when((col(TempColumn.TIME_PERIOD_INDEX) - col(TempColumn.DOSE_3)) < time_period_amount)
                    .then(lit(VaccineStatus.LESS_THAN_4_WEEKS_FROM_DOSE_3))
                    .otherwise(lit(VaccineStatus.MORE_THAN_4_WEEKS_FROM_DOSE_3))
                )
                .otherwise(
                    when(col(TempColumn.DOSE_2).is_not_null() & (col(TempColumn.TIME_PERIOD_INDEX) >= col(TempColumn.DOSE_2)))
                    .then(
                        when((col(TempColumn.TIME_PERIOD_INDEX) - col(TempColumn.DOSE_2)) < time_period_amount)
                        .then(lit(VaccineStatus.LESS_THAN_4_WEEKS_FROM_DOSE_2))
                        .otherwise(lit(VaccineStatus.MORE_THAN_4_WEEKS_FROM_DOSE_2))
                    )
                    .otherwise(
                        when(col(TempColumn.DOSE_1).is_not_null() & (col(TempColumn.TIME_PERIOD_INDEX) >= col(TempColumn.DOSE_1)))
                        .then(
                            when((col(TempColumn.TIME_PERIOD_INDEX) - col(TempColumn.DOSE_1)) < time_period_amount)
                            .then(lit(VaccineStatus.LESS_THAN_4_WEEKS_FROM_DOSE_1))
                            .otherwise(lit(VaccineStatus.MORE_THAN_4_WEEKS_FROM_DOSE_1))
                        )
                        .otherwise(lit(VaccineStatus.UNVACCINATED))
                    )
                )
            )
        )
