from datetime import datetime, timedelta


class TimePeriodHelper:
    @staticmethod
    def get_weeks_in_range(start_date: datetime, end_date: datetime) -> list[str]:
        weeks = []

        current_date = start_date

        while current_date <= end_date:
            year, week, _ = current_date.isocalendar()
            weeks.append(f"{year}W{str(week).zfill(2)}")
            current_date += timedelta(days=7)

        return sorted(set(weeks))

    @staticmethod
    def get_months_in_range(start_date: datetime, end_date: datetime) -> list[str]:
        months = []

        current_year = start_date.year
        current_month = start_date.month

        while (current_year, current_month) <= (end_date.year, end_date.month):
            months.append(f"{current_year}M{str(current_month).zfill(2)}")

            if current_month == 12:
                current_month = 1
                current_year += 1
            else:
                current_month += 1

        return months
