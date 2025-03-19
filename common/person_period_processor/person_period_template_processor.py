from polars import DataFrame, col

from common.person_period_template_generator.person_period_template_generator import PersonPeriodDfSchema
from common.polars_expressions.age_group_expression import AgeGroupExpression
from common.polars_expressions.death_status_expression import DeathStatusExpression
from common.polars_expressions.vaccine_status_expression import VaccineStatusExpression
from common.typings import AgeStatus, NewColumn


class PersonPeriodTemplateProcessor:
    def process(self, df: DataFrame, is_using_months: bool = False) -> DataFrame:
        assert df.schema == PersonPeriodDfSchema

        time_period_amount = 1 if is_using_months else 4

        return (
            df.with_columns(
                [
                    AgeGroupExpression.get_expr().alias(NewColumn.AGE),
                    DeathStatusExpression.get_expr().alias(NewColumn.DEATH_STATUS),
                    VaccineStatusExpression.get_expr(time_period_amount).alias(NewColumn.VACCINE_STATUS),
                ]
            )
            .filter(col(NewColumn.AGE) != AgeStatus.LESS_THAN_60)  # It's not really necessary, but it saves some RAM :)
            .select(list(NewColumn))
        )
