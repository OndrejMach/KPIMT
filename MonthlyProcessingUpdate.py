from functions import date_to_month
from functions import update_kpi_name

import pandas as pd

class MonthlyProcessing:
    input=None
    daily=None
    kpis=None

    def __init__(self, input, daily, kpis):
        self.input=input
        self.daily=daily
        self.kpis=kpis


    def process_data(self):
        daily_data = self.daily.loc[self.daily['Input_ID'] == 'L'].copy()

        daily_data['Date'] = daily_data['Date'].map(date_to_month)

        joined = pd.merge(daily_data, self.kpis, left_on="KPI_ID", right_on="EDWH_KPI_ID")

        joined['KPI_name'] = [x.upper() for x in joined['EDWH_KPI_Name']]

        joined = joined.apply(lambda row: update_kpi_name(row), axis=1)

        joined.set_index(["Region", "Date", "KPI_name"])

        result = joined \
            .groupby(["Region", "Date", "KPI_name"]) \
            .agg(value=("Value", "mean"), was_corrected_Flag=("was_corrected_Flag", "min")) \
            .reset_index()
        result['Input_ID'] = result.apply(lambda x: "{}-{}-{}-m".format(x['Region'], x['KPI_name'], x['Date']), axis=1)
        result['Remarks'] = ""
        result['Input_File'] = "calculation"

        return result

