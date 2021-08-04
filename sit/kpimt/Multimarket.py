import re

import pandas as pd
import time
from datetime import datetime
from sit.kpimt.confs import corections_schema, output_schema
from sit.kpimt.matrixFunctions import iso_mapping

class Multimarket:
    all_monthly = None
    multimarket_in = None
    filename = None
    filetime = None
    corrections = None
    datetime_format = None

    def __init__(self, all_monthly, multimarket_in, filename, filetime, corrections, datetime_format = "%d.%m.%Y"):
        self.all_monthly = all_monthly
        self.multimarket_in = multimarket_in
        self.filetime = filetime
        self.filename = filename
        self.corrections = corrections
        self.datetime_format = datetime_format


    def process_data(self):
        def get_input_id(row):
            row['Input_ID'] = "{}-{}-{}-m".format(row['Region'], row['KPINameMonthly'], row['DatumKPI'])
            return row
        def evaluate_value(row):
            if (pd.isna(row['KPIValue_Compare'])):
                row['KPIValue_Compare']= row['KPIValue']
            return row
        def evaluate_remarks(row):
            if (pd.isna(row['KPIValue_Compare'])):
                row['KPIValue_Compare']= row['KPIValue']
            return row

        def get_date(date):
            date_f = str(date).split(sep=" ")[0]
            date_format = "%d.%m.%Y" if (re.match("\d{2}.\d{2}.\d{4}", date_f)) else "%Y-%m-%d"
            return datetime.strptime(date_f, date_format).strftime("%d.%m.%Y")



        input = self.multimarket_in[self.multimarket_in['Value'].notna()].copy()
        input['KPINameMonthly'] = input['KPI name'].apply(lambda x: str(x).upper().strip())
        input['DatumKPI'] = input['Date'].map(lambda x: get_date(x))
        input['Region'] = input['Region'].apply(lambda x: iso_mapping[str(x)])
        input['KPIValue'] = input['Value'].apply(lambda x: str(x).replace(",", "."))
        input['num_values'] = input['KPIValue'].astype(str).str.contains("[a-zA-Z]", regex=True)
        input = input[
            input['KPIValue'].astype(str).str.contains("[0-9\.,]", regex=True) & (input['num_values'] != True)]
        input['KPIValue'] = input['KPIValue'].apply(lambda x: float(x))
        input['Input_File'] =self.filename
        input['Input_File_Time'] = self.filetime
        input['Input_ID'] = None
        input = input.apply(lambda row: get_input_id(row), axis = 1)
        input = input[['Input_ID','DatumKPI', 'KPINameMonthly', 'Region', 'KPIValue', 'Remarks', 'Input_File', 'Input_File_Time' ]]
        input_corr = input.copy()
        input_corr['KPIValueNew'] = input_corr['KPIValue']

        monthly = self.all_monthly[self.all_monthly['Input_ID'].notna()].copy()
        monthly.rename(columns={"Value": "KPIValueOld"}, inplace=True)
        value_map = monthly[['Input_ID', 'KPIValueOld']].drop_duplicates()
        input_file_map = monthly[['Input_ID', 'Input_File']].drop_duplicates()
        input_file_map.rename(columns={"Input_File" : "Input_File_Old"}, inplace=True)
        remarks_map=monthly[['Input_ID', 'Remarks']].drop_duplicates()
        remarks_map.rename(columns={"Remarks" : "Remarks_Old"}, inplace = True)

        input2 = pd.merge(input_corr,value_map, how="left", on="Input_ID" )
        input2['KPIValue_Compare'] = input2['KPIValueOld']
        input2 = input2.apply(lambda row: evaluate_value(row), axis=1)
        input2 = pd.merge(input2,input_file_map, how="left", on="Input_ID" )
        input2 = pd.merge(input2,remarks_map,how="left", on="Input_ID" )
        input2['Remarks_Compare'] = input2['Remarks_Old']
        input2 = input2.apply(lambda row: evaluate_remarks(row), axis=1)

        input2['remarks_check'] = input2.apply(lambda x: str(x['Remarks']) in str(x['Remarks_Compare']),
                                                    axis=1)

        update_corr = input2[(input2['KPIValueNew'] != input2['KPIValue_Compare']) | (input2['remarks_check'] != True)]\
            .rename(columns = {"Input_ID":"Key_Corr",
                               "Remarks_Old": "CommentRowOld",
                               "Remarks": "CommentRowNew",
                               "Input_File_Time": "TimestampCorrFile",
                               "Input_File_Old": "FileOld",
                               "Input_File" : "FileNew",
                               "KPINameMonthly":"KPINameQVD",
                               "Region" : "Natco"
                               })
        update_corr['Granularity'] = 'M'
        update_corr['CommentFileOld'] = None
        update_corr['CommentFileNew'] = None
        update_corr['correction_timestamp'] = time.time()
        update_corr = update_corr[corections_schema]

        corr = self.corrections[corections_schema]

        result_corrections = pd.concat([corr, update_corr])[corections_schema]

        input_multimarket = input.rename(columns={'DatumKPI':'Date', 'KPINameMonthly': 'KPI name','KPIValue':'Value'})[['Input_ID','Date','KPI name','Region','Value', 'Remarks', 'Input_File']]
        corrections_map = result_corrections[result_corrections['Granularity'] == 'M'][['Key_Corr']].drop_duplicates()
        corrections_map['was_corrected_Flag'] = 'Correction'
        monthly2=pd.merge(input_multimarket,corrections_map, left_on='Input_ID', right_on='Key_Corr', how="left" )
        monthly2.drop(columns=['Key_Corr'], inplace=True)
        all_monthly = self.all_monthly.copy()

        monthly_filter = monthly2[['Input_ID']].drop_duplicates()
        monthly_filter['exists_in_monthly'] = 1
        all_monthly_filtered = pd.merge(all_monthly,monthly_filter, on=['Input_ID'], how="left" )
        all_monthly_filtered = all_monthly_filtered[all_monthly_filtered['exists_in_monthly'].isnull() | all_monthly_filtered['exists_in_monthly'].isna()]
        monthly2 = pd.concat([monthly2, all_monthly_filtered])[output_schema]

        return {"corrections": result_corrections,
                "TMA" : monthly2[monthly2['Region'] == 'TMA'],
                "TMCZ": monthly2[monthly2['Region'] == 'TMCZ'],
                "TMHR": monthly2[monthly2['Region'] == 'TMHR'],
                "COSROM": monthly2[monthly2['Region'] == 'COSROM']
                }





















