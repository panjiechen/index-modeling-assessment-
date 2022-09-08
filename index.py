import datetime as dt
import pandas as pd
import os
import collections


class IndexModel:

    def __init__(self) -> None:
        # To be implemented
        self.output_data = pd.DataFrame()
        self.input_data = pd.DataFrame()
        self.selected = {}
        self.previous_index = None
        self.previous_return = None

    def calc_index_level(self, start_date: dt.date, end_date: dt.date) -> None:
        # To be implemented
        # Read from CSV
        self.get_data("stock_prices")

        index_df = pd.DataFrame()
        ready_for_new_month = False
        month_of_yesterday = None
        data_of_yesterday = None
        for index, row in self.input_data.sort_index().iterrows():
            sorted_row = row.sort_values(ascending=False)
            month = index.month
            pre_month = (index - pd.DateOffset(months=1)).month
            two_month_ago = (index - pd.DateOffset(months=2)).month
            self.selected[month] = sorted_row.iloc[:3].copy()

            # If previous index does not exist, it means it hasn't start yet, keep update the return of last day.
            if self.previous_index is None:
                self.previous_return = self.cal_return(sorted_row.iloc[:3].copy())

            # If we seleted top 3 for the last month, start calculation
            if self.selected.get(pre_month) is not None:
                current_data = collections.OrderedDict()
                data_choice = self.selected[pre_month].items()

                # First day of the month, need to use previous top 3 selection
                if not ready_for_new_month and month_of_yesterday != month and self.selected.get(
                        two_month_ago) is not None:
                    data_choice = self.selected[two_month_ago].items()
                for k, v in data_choice:
                    current_data[k] = row[k]

                # Second day of the month, so the previous total return need to be re-calculated with the
                # newer top 3 selections.
                if data_of_yesterday is not None and ready_for_new_month:
                    re_cal_data = collections.OrderedDict()
                    for k, v in self.selected[pre_month].items():
                        re_cal_data[k] = data_of_yesterday[k]
                    self.previous_return = self.cal_return(re_cal_data)
                    ready_for_new_month = False

                # Calculate current return and index of the day
                current_return = self.cal_return(current_data)
                current_index = self.get_index(current_return) if self.previous_index else 100

                # Adding data to the result
                new_row = pd.DataFrame({"index_level": round(current_index, 2)}, index=[index])
                index_df = pd.concat([new_row, index_df.loc[:]])

                # Update cached data for further calculation
                self.previous_index = current_index
                self.previous_return = current_return

                # If this is the first day of the month, need to use new top 3 selection after
                if index.month != month_of_yesterday:
                    ready_for_new_month = True

                # Update cached data for further calculation
                data_of_yesterday = row.copy()
                month_of_yesterday = index.month
        self.output_data = index_df.sort_index().copy()[start_date: end_date]
        self.output_data.index.name = "Date"
        # print(self.output_data.to_string())

    def export_values(self, file_name: str) -> None:
        # To be implemented
        self.output_data.to_csv(file_name, date_format='%d/%m/%Y')
        pass

    def get_data(self, file_name):
        # dates = pd.date_range(dt.date(year=2019, month=12, day=30), end_date)
        df_tmp = pd.read_csv(
            self.get_path(file_name),
            index_col="Date",
            parse_dates=["Date"],
            dayfirst=True
        )

        self.input_data = df_tmp

    def get_path(self, file_name, base_dir=None):
        if base_dir is None:
            base_dir = os.path.join(os.getcwd(), "data_sources")
        return os.path.join(base_dir, f"{file_name}.csv")

    def get_index(self, current_return):
        return self.previous_index * (current_return / self.previous_return)

    def cal_return(self, series_data):
        i = 0
        r = 0
        for k, v in series_data.items():
            r += v * (0.5 if i == 0 else 0.25)
            i += 1
        return r
