from datetime import datetime

import matplotlib.pyplot as plt
from polars import DataFrame, col

from common.acm_calculator.acm_calculator import ACMCalculator
from common.file_storage.dataframe_storage import ArrowPolarsDataframeStorage
from common.file_storage.file_storage import LocalFileStorage
from common.time_period.time_period_helper import TimePeriodHelper
from common.time_tracker import TimeTracker
from common.typings import COLORS, AgeStatus, VaccineStatus


class GraphMaker:
    def __init__(self, acm_calculator: ACMCalculator) -> None:
        self.__acm_calculator = acm_calculator

        self.__vaccine_labels: list[VaccineStatus] = []
        self.__acms_by_age: dict[AgeStatus, list[float]] = {}
        self.__deaths_by_age: dict[AgeStatus, list[int]] = {}

    def prepare_data(self, df: DataFrame, from_date: datetime, to_date: datetime) -> None:
        is_using_months = self.__is_df_using_months(df)

        time_periods = (
            TimePeriodHelper.get_months_in_range(from_date, to_date)
            if is_using_months
            else TimePeriodHelper.get_weeks_in_range(from_date, to_date)
        )

        df = df.filter(col("time_period").is_in(time_periods))

        analysed_age_statuses = sorted(df["age"].unique().to_list())
        analysed_vaccine_statuses = sorted(df["vaccine_status"].unique().to_list())

        for age_status in analysed_age_statuses:
            if age_status not in self.__acms_by_age or age_status not in self.__deaths_by_age:
                self.__acms_by_age[age_status] = []
                self.__deaths_by_age[age_status] = []
            for vac_status in analysed_vaccine_statuses:
                result = self.__acm_calculator.compute_person_years_acm(df, age_status, vac_status, is_using_months)
                self.__acms_by_age[age_status].append(result.acm)
                self.__deaths_by_age[age_status].append(result.total_deaths)
                print("Calculating:", age_status, vac_status)

        self.__vaccine_labels = analysed_vaccine_statuses

    def draw_simple_bar_chart(self, output_file: str) -> None:
        x_positions = range(len(self.__acms_by_age.keys()))
        bar_width = 0.04
        bar_gap = 0.02

        _, ax = plt.subplots(figsize=(10, 10))

        for i, (vac_status, color) in enumerate(zip(self.__vaccine_labels, COLORS)):
            x_offsets = [x + i * (bar_width + bar_gap) for x in x_positions]
            heights = [values[i] for _, values in self.__acms_by_age.items()]

            bars = ax.bar(
                x_offsets,
                heights,
                bar_width,
                label=vac_status,
                color=color,
                edgecolor="black",
            )

            for j, single_bar in enumerate(bars):
                total_deaths = self.__deaths_by_age[list(self.__acms_by_age.keys())[j]][i]
                ax.text(
                    single_bar.get_x() + single_bar.get_width() / 2,
                    single_bar.get_height() + 10,
                    str(total_deaths),
                    ha="center",
                    va="bottom",
                    color="black",
                    fontsize=9,
                )

        ax.grid(axis="y", color="black", linewidth=0.5)

        ax.set_xticks([x + bar_width * (len(self.__vaccine_labels) / 2) for x in x_positions])
        ax.set_xticklabels(self.__acms_by_age.keys())
        ax.set_ylabel("Number of deaths per 100,000 person-years")
        ax.legend(title="Vaccination Status")

        plt.tight_layout()
        plt.savefig(output_file)

    def __is_df_using_months(self, df: DataFrame) -> bool:
        return all(df["time_period"].str.contains(r"^\d{4}M\d{2}$"))


FROM_DATE = datetime(2021, 1, 1)
TO_DATE = datetime(2022, 12, 30)
FILE_NAME = "CPZP_from_2020_to_2022"
OUTPUT_FILE_NAME = "CPZP_test"


def main() -> None:
    file_storage = ArrowPolarsDataframeStorage(LocalFileStorage(), "./")
    with TimeTracker("FileStorage read cpzp_processed_df"):
        df = file_storage.read(f"./data/{FILE_NAME}")

    calculator = ACMCalculator()
    graph_maker = GraphMaker(calculator)
    graph_maker.prepare_data(df, FROM_DATE, TO_DATE)
    graph_maker.draw_simple_bar_chart(f"./out/{OUTPUT_FILE_NAME}.png")


if __name__ == "__main__":
    main()
