import random
from datetime import datetime

from polars import DataFrame, Int64, col

from common.file_storage.dataframe_storage import ArrowPolarsDataframeStorage
from common.file_storage.file_storage import LocalFileStorage
from common.person_period_processor.person_period_template_processor import PersonPeriodTemplateProcessor
from common.person_period_template_generator.person_period_template_generator import TempColumn
from common.time_period.time_period_helper import TimePeriodHelper
from common.time_tracker import TimeTracker


class Individual:
    def __init__(self, person_id: int) -> None:
        self.__id = person_id
        self.__death_index = self.__assign_death()
        self.__dose_indexes: list[int] = []

    def add_dose(self, week_index: int) -> bool:
        if self.__should_receive_dose(week_index):
            self.__dose_indexes.append(week_index)
            return True

        return False

    def to_person_period_schema(self) -> dict[TempColumn, int | str | None]:
        return {
            TempColumn.PERSON_ID: self.__id,
            TempColumn.BIRTHDATE: "1900-01-05",  # mocked birthdate for 80+ y.o. group
            TempColumn.DOSE_1: (self.__dose_indexes[0] if len(self.__dose_indexes) > 0 else None),
            TempColumn.DOSE_2: (self.__dose_indexes[1] if len(self.__dose_indexes) > 1 else None),
            TempColumn.DOSE_3: (self.__dose_indexes[2] if len(self.__dose_indexes) > 2 else None),
            TempColumn.DOSE_4: NUM_OF_WEEKS + 1,
            TempColumn.DEATH_INDEX: self.__death_index,
        }

    def __assign_death(self) -> int:
        if random.uniform(0, 1) <= DEATH_PROBABILITY:
            return round(random.uniform(0, NUM_OF_WEEKS))
        return NUM_OF_WEEKS + 1

    def __should_receive_dose(self, week_index: int) -> bool:
        if self.__death_index - week_index <= HVE_WINDOW:
            return random.uniform(0, 1) > HVE_PROBABILITY
        return True


class CohortSimulator:
    def __init__(self) -> None:
        self.__population = self.__initialize_population()

    def assign_doses(self) -> None:
        for person in self.__population:
            if random.uniform(0, 1) < 0.82:
                dose1_week = round(random.gauss(20, 3))
                can_recieve_next = person.add_dose(dose1_week)

                if can_recieve_next and random.uniform(0, 1) < 0.96:
                    dose2_week = dose1_week + round(random.gauss(20, 3))
                    can_recieve_next = person.add_dose(dose2_week)

                    if can_recieve_next and random.uniform(0, 1) < 0.82:
                        dose3_week = dose2_week + round(random.gauss(20, 3))
                        person.add_dose(dose3_week)

    def to_polars_df(self) -> DataFrame:
        persons_df = DataFrame(
            [p.to_person_period_schema() for p in self.__population],
        )

        weeks_df = DataFrame(
            {
                TempColumn.TIME_PERIOD: TIME_SPAN,
                TempColumn.TIME_PERIOD_INDEX: list(range(len(TIME_SPAN))),
            }
        )

        return persons_df.with_columns(col(TempColumn.PERSON_ID).cast(Int64)).join(weeks_df, how="cross")

    def __initialize_population(self) -> list[Individual]:
        return [Individual(i) for i in range(1, NUMBER_OF_INDIVIDUALS)]


NUMBER_OF_INDIVIDUALS = 1_700_000
DEATH_PROBABILITY = 1 / 10

TIME_SPAN = TimePeriodHelper.get_weeks_in_range(datetime.fromisoformat("2021-01-05"), datetime.fromisoformat("2022-12-31"))

NUM_OF_WEEKS = len(TIME_SPAN)
HVE_WINDOW = 26
HVE_PROBABILITY = 0.0


def main() -> None:

    file_storage = ArrowPolarsDataframeStorage(LocalFileStorage(), "./data/")

    cohort_simulator = CohortSimulator()
    person_period_template_processor = PersonPeriodTemplateProcessor()

    with TimeTracker("Running simulation"):
        cohort_simulator.assign_doses()

    with TimeTracker("Convert simulation results to dataframe"):
        person_period_template = cohort_simulator.to_polars_df()

    with TimeTracker("Processing person period template"):
        processed_df = person_period_template_processor.process(person_period_template)

    file_storage.write(f"HVE-{HVE_PROBABILITY}-simulation", processed_df)


if __name__ == "__main__":
    main()
