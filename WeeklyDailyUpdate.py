from datetime import datetime
import pandas as pd
from functions import update_kpi_name

class DailyWeeklyUpdate:
    kpis=None
    dailyOutput=None
    weeklyOutput=None

    def __init__(self, kpis, dailyOutput,weeklyOutput ):
        self.kpis=kpis
        self.dailyOutput=dailyOutput
        self.weeklyOutput=weeklyOutput

    def process_data(self):
        def corrections_correction(x):
            if (x == "Correction"):
                return "Correction(s)"
            else:
                return x

        if (self.dailyOutput is not None):
            today = datetime.date.today()
            last_monday = today - datetime.timedelta(days=today.weekday())
            daily_tmp = self.dailyOutput.copy()
            daily_tmp['Date'] = last_monday.strftime("%d.%m.%Y")
            kpis_tmp = self.kpis.copy()

            kpis_tmp['EDWH_KPI_ID'] = kpis_tmp['EDWH_KPI_ID'].map(lambda x: str(x).upper())
            joined_kpis = pd.merge(daily_tmp,kpis_tmp, left_on="KPI_ID", right_on="EDWH_KPI_ID")
            joined_kpis['KPI name'] = joined_kpis['EDWH_KPI_Name'].map(lambda x: str(x).upper())
            joined_kpis = joined_kpis.apply(lambda row: update_kpi_name(row))
            avg_proj = joined_kpis[["Date"], ["Region"], ["KPI_ID"], ["Value"], ["KPI Name"], ["was_corrected_Flag"]]
            avg_tmp = avg_proj \
                .groupby(["Region", "Date", "KPI name"]) \
                .agg(value=("Value", "mean"), was_corrected_Flag=("was_corrected_Flag", "min")) \
                .reset_index()
            avg_tmp['was_corrected_Flag'] = avg_tmp['was_corrected_Flag'].map(lambda x: corrections_correction(x))
            avg_tmp['Input_ID'] = avg_tmp.apply(lambda x: "{}-{}-{}-m".format(x['Region'], x['KPI_name'], x['Date']),axis=1)
            avg_tmp['Remarks'] = ""
            avg_tmp['Input_File'] = "calculation"
            avg_tmp['sortTimestamp'] = pd.to_datetime(avg_tmp['Date'], format="%d.%m.%Y")
            avg_tmp.sort_values(by=['sortTimestamp'])
            return avg_tmp[["Input_ID"], ["Date"], ["KPI Name"], ["Region"], ["Value"], ["Remarks"], ["Input_File"], ["was_corrected_Flag"]]
        else:
            return pd.DataFrame(columns = ["Input_ID","Date", "KPI Name", "Region", "Value", "Remarks", "Input_File", "was_corrected_Flag"])



