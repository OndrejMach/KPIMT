from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import re
from sit.kpimt.functions import isfloat

class Monthly_avgs:
    kpis=None
    dailyOutput=None
    monthlyOutput=None

    def __init__(self, kpis, dailyOutput,monthlyOutput ):
        self.kpis=kpis
        self.dailyOutput=dailyOutput
        self.monthlyOutput=monthlyOutput

    def process_data(self):
        def fix_null(row):
            if (pd.isna(row['EDWH_KPI_Name'])):
                row['EDWH_KPI_Name'] = '*nonamefor_'+str(row['KPI_ID']).upper()
            return row

        def get_month_start(date):
            if (re.match("\d{2}\.\d{2}\.\d{4}", date)):
                today = datetime.strptime(date, '%d.%m.%Y')
                ret = today.strftime("01.%m.%Y")
            #print("MONTHLY DATE: " + ret)
                return ret
            else:
                return None
        def get_input_id(row):
            row['Input_ID'] = "{}-{}-{}-m".format(row['Region'], row['KPI name'], row['Date'])
            return row

        kpis_map = self.kpis.drop_duplicates()
        kpis_map.rename(columns={'EDWH_KPI_ID':'KPI_ID'}, inplace=True)


        daily_output = self.dailyOutput.copy()
        daily_output['Date'] = daily_output['Date'].apply(lambda x: get_month_start(str(x)))
        daily_output['KPI_ID'] = daily_output['KPI_ID'].apply(lambda x: str(x).upper())
        avg_tmp = pd.merge(daily_output,kpis_map,on="KPI_ID",how="left")
        avg_tmp = avg_tmp.apply(lambda x: fix_null(x), axis=1)
        avg_tmp.rename(columns={'EDWH_KPI_Name':'KPI name'}, inplace=True)
        avg_tmp['num_values'] = avg_tmp['Value'].apply(lambda x: isfloat(x))
        avg_tmp = avg_tmp[avg_tmp['num_values'] != False]
        avg_tmp.drop(columns=['num_values'], inplace=True)
        avg_tmp['Value'] = avg_tmp['Value'].apply(lambda x: float(x))
        avg_tmp['was_corrected_Flag'].fillna('', inplace=True)

        print(avg_tmp.columns)

        averages = avg_tmp.groupby(['Region','Date','KPI name']).agg(Value=("Value", 'mean'),was_corrected_Flag=('was_corrected_Flag', np.min)).reset_index()
        averages['was_corrected_Flag'] = averages['was_corrected_Flag'].apply(lambda x: 'Correction(s)' if (x == 'Correction') else '')
        averages['Input_ID'] = None
        averages= averages.apply(lambda x: get_input_id(x), axis=1)
        averages['Remarks'] = ''
        averages['Input_File']='calculation'
        averages = averages[["Input_ID","Date",'KPI name',"Region","Value","Remarks","Input_File","was_corrected_Flag"]]
        print(averages.columns)
        print("AGGREGATES COUNT: "+str(averages.shape[0]))
        print(averages.info)

        data0 = self.monthlyOutput[self.monthlyOutput['Input_File'] != 'calculation'].copy()
        data0 = data0[['Input_ID']].drop_duplicates()
        data0['is_in_weekly'] = 1

        avgs_new = pd.merge(averages,data0, on="Input_ID", how="left")
        avgs_new = avgs_new[np.isnan(avgs_new['is_in_weekly'])]
        avgs_new = avgs_new[["Input_ID","Date",'KPI name',"Region","Value","Remarks","Input_File","was_corrected_Flag"]]

        avgs_filter =avgs_new[['Input_ID']].drop_duplicates()
        avgs_filter['is_not_calculated'] = 1

        monthly_out = self.monthlyOutput.copy()
        result_filtered = pd.merge(monthly_out,avgs_filter, on="Input_ID", how="left")
        result_monthly = result_filtered[pd.isna(result_filtered['is_not_calculated'])]
        result_monthly = result_monthly[["Input_ID","Date",'KPI name',"Region","Value","Remarks","Input_File","was_corrected_Flag"]]
        result= pd.concat([avgs_new, result_monthly])

        print(result.columns)
        print("MONTHLY OUT COUNT: " + str(result.shape[0]))
        print(result.info)

        return {'averages': averages, 'out' : result}


