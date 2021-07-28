from datetime import datetime, timedelta
import pandas as pd
import numpy as np

class Weekly_avgs:
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


        def get_week_start():
            today = datetime.today()
            last_monday = today - timedelta(days=today.weekday())
            ret = last_monday.strftime("%d.%m.%Y")
            print("WEEKLY DATE: "+ret)
            return ret

        kpis_map = self.kpis.drop_duplicates()
        kpis_map.rename(columns={'EDWH_KPI_ID':'KPI_ID'}, inplace=True)


        daily_output = self.dailyOutput.copy()
        daily_output['Date'] = get_week_start()
        daily_output['KPI_ID'] = daily_output['KPI_ID'].apply(lambda x: str(x).upper())
        avg_tmp = pd.merge(daily_output,kpis_map,on="KPI_ID",how="left")
        avg_tmp.rename(columns={'EDWH_KPI_Name':'KPI name'}, inplace=True)
        avg_tmp['Value'] = avg_tmp['Value'].apply(lambda x: float(x))
        avg_tmp['was_corrected_Flag'].fillna('', inplace=True)

        print(avg_tmp.columns)

        averages = avg_tmp.groupby(['Region','Date','KPI name']).agg(Value=("Value", 'mean'),was_corrected_Flag=('was_corrected_Flag', np.min)).reset_index()
        averages['was_corrected_Flag'] = averages['was_corrected_Flag'].apply(lambda x: 'Correction(s)' if (x == 'Correction') else '')
        averages['Input_ID'] = averages.apply(lambda x: "{}-{}-{}-w".format(x['Region'], x['KPI name'], x['Date']), axis=1)
        averages['Remarks'] = ''
        averages['Input_File']='calculation'
        averages = averages[["Input_ID","Date",'KPI name',"Region","Value","Remarks","Input_File","was_corrected_Flag"]]
        print(averages.columns)
        print("AGGREGATES COUNT: "+str(averages.shape[0]))
        print(averages.info)

        data0 = self.weeklyOutput[self.weeklyOutput['Input_File'] != 'calculation'].copy()
        data0 = data0[['Input_ID']].drop_duplicates()
        data0['is_in_weekly'] = 1

        avgs_new = pd.merge(averages,data0, on="Input_ID", how="left")
        avgs_new = avgs_new[np.isnan(avgs_new['is_in_weekly'])]
        avgs_new = avgs_new[["Input_ID","Date",'KPI name',"Region","Value","Remarks","Input_File","was_corrected_Flag"]]

        avgs_filter =avgs_new[['Input_ID']].drop_duplicates()
        avgs_filter['is_not_calculated'] = 1

        weekly_out = self.weeklyOutput.copy()
        result_filtered = pd.merge(weekly_out,avgs_filter, on="Input_ID", how="left")
        result_weekly = result_filtered[pd.isna(result_filtered['is_not_calculated'])]
        result_weekly = result_weekly[["Input_ID","Date",'KPI name',"Region","Value","Remarks","Input_File","was_corrected_Flag"]]
        result= pd.concat([avgs_new, result_weekly])

        print(result.columns)
        print("WEEKLY OUT COUNT: " + str(result.shape[0]))
        print(result.info)

        return {'averages': averages, 'weekly_out' : result}


