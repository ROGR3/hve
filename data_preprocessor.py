from datetime import datetime

from polars import read_csv

from common.file_storage.dataframe_storage import ArrowPolarsDataframeStorage
from common.file_storage.file_storage import LocalFileStorage
from common.person_period_processor.person_period_template_processor import PersonPeriodTemplateProcessor
from common.person_period_template_generator.cpzp_person_period_template_generator import CpzpPersonPeriodTemplateGenerator
from common.person_period_template_generator.ozp_person_period_template_generator import OzpPersonPeriodTemplateGenerator
from common.time_tracker import TimeTracker

FROM_DATE = datetime(2020, 1, 1)
TO_DATE = datetime(2022, 12, 30)


def main() -> None:
    cpzp_person_week_generator = CpzpPersonPeriodTemplateGenerator(FROM_DATE, TO_DATE)
    ozp_person_month_generator = OzpPersonPeriodTemplateGenerator(FROM_DATE, TO_DATE)
    processor = PersonPeriodTemplateProcessor()
    file_storage = ArrowPolarsDataframeStorage(LocalFileStorage(), "./data/")

    cpzp_df = read_csv("./data/CPZP.csv", separator=";")
    ozp_df = read_csv("./data/OZP.csv", separator=";")

    with TimeTracker("Preprocessing CPZP dataframe"):
        cpzp_df = cpzp_person_week_generator.generate(cpzp_df)
        cpzp_processed_df = processor.process(cpzp_df, is_using_months=False)

    with TimeTracker("Preprocessing OZP dataframe"):
        ozp_df = ozp_person_month_generator.generate(ozp_df)
        ozp_processed_df = processor.process(ozp_df, is_using_months=True)

    with TimeTracker("Writing both dataframes"):
        file_storage.write(f"CPZP_from_{FROM_DATE.year}_to_{TO_DATE.year}", cpzp_processed_df)
        file_storage.write(f"OZP_from_{FROM_DATE.year}_to_{TO_DATE.year}", ozp_processed_df)


if __name__ == "__main__":
    main()
