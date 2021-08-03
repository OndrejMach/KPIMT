import re

import pandas as pd
from datetime import datetime
import time

class Weekly_input:
    weekly_output=None
    raw_input=None
    corrections=None
    natco=None
    filename=None
    filename_timestamp=None
    datetime_format = '%Y-%m-%d %H:%M:%S'


    def __init__(self, weekly_output,raw_input, corrections, natco, filename, filename_timestamp, datetime_format = '%d.%m.%Y'):
        self.weekly_output=weekly_output
        self.raw_input=raw_input
        self.corrections=corrections
        self.natco=natco
        self.filename=filename
        self.filename_timestamp=filename_timestamp
        self.datetime_format = datetime_format


    def process_data(self):
        def get_input_id(row):
            row['Input_ID'] = "{}-{}-{}-w".format(row['Region'], row['KPINameWeekly'], row['DatumKPI'])
            return row

        input =self.raw_input[(self.raw_input['Value'].notna())].copy()
        input['DatumKPI'] = input['Date'].map(lambda x: datetime.strptime(str(x),self.datetime_format).strftime("%d.%m.%Y"))
        input['Natco'] = self.natco
        input['Region'] = self.natco
        input['KPINameWeekly'] = input['KPI name'].map(lambda x: str(x).upper().strip())
        input['Input_File'] = self.filename
        input['Input_File_Time'] = self.filename_timestamp
        input['Input_ID'] = None
        input = input.apply(lambda x: get_input_id(x),axis=1)
        input['KPIValue'] = input['Value'].apply(lambda x: str(x).replace(",","."))
        input  = input[input['KPIValue'].astype(str).str.contains("[0-9\.,]", regex=True)]
        input['KPIValue'] = input['KPIValue'].apply(lambda x: float(x))
        input['Value'] = input['KPIValue']

        input_raw = input[['Input_ID','DatumKPI', 'KPINameWeekly', 'Region', 'Value','Remarks','Input_File' ]].copy()
        input_raw.rename(columns={'DatumKPI':'Date','KPINameWeekly':'KPI name' }, inplace= True)
        input_raw = input_raw[['Input_ID', 'Date', 'KPI name', 'Region','Value','Remarks','Input_File']]

        input = input[['Input_ID','DatumKPI','KPINameWeekly','Region','KPIValue','Remarks','Input_File','Input_File_Time']]

        print(input.columns)
        print("INPUT COUNT: " + str(input.shape[0]))
        print(input.info)

        weekly_output = self.weekly_output[self.weekly_output['Input_ID'].notna()][['Input_ID', 'Value', 'Remarks','Input_File']].copy()
        weekly_output.rename(columns = {'Value':'KPIValueOld'}, inplace=True)
        print(weekly_output.columns)
        print("INPUT COUNT: " + str(weekly_output.shape[0]))
        print(weekly_output.info)

        value_map = weekly_output[['Input_ID', 'KPIValueOld']].drop_duplicates()
        input_file_map = weekly_output[['Input_ID', 'Input_File']].drop_duplicates()
        input_file_map.rename(columns={"Input_File":"Input_File_Old"}, inplace = True)
        remarks_map = weekly_output[['Input_ID', 'Remarks']].drop_duplicates()
        remarks_map.rename(columns = {"Remarks":"Remarks_Old"}, inplace = True)

        weekly2 = pd.merge(input, value_map, on="Input_ID", how="left")
        weekly2 = pd.merge(weekly2, input_file_map, on="Input_ID", how="left")
        weekly2 = pd.merge(weekly2, remarks_map, on="Input_ID", how="left")

        weekly2['KPIValue_Compare'] = weekly2.apply(lambda x: x['KPIValue'] if (pd.isna(x['KPIValueOld']) )else x['KPIValueOld'], axis=1)
        weekly2['Remarks_Compare'] = weekly2.apply(
            lambda x: x['Remarks'] if (pd.isna(x['KPIValueOld'])) else x['Remarks_Old'], axis=1)
        weekly2.rename(columns={"KPIValue":"KPIValueNew"}, inplace=True)
        weekly2 = weekly2[["Input_ID","DatumKPI","KPINameWeekly","Region","KPIValueNew","Remarks","Input_File","Input_File_Time","KPIValueOld","KPIValue_Compare","Input_File_Old","Remarks_Old","Remarks_Compare"]]
        weekly2.rename(columns={"Input_ID":"Key_Corr","Remarks_Old":"CommentRowOld", "Remarks":"CommentRowNew","Input_File_Time":"TimestampCorrFile","Input_File_Old":"FileOld","Input_File":"FileNew"}, inplace=True)
        weekly2['CommentFileOld'] = None
        weekly2['CommentFileNew'] = None
        weekly2['Granularity'] = "W"
        weekly2['Comments_check'] = weekly2.apply(lambda x: str(x['CommentRowNew']) in str(x['Remarks_Compare']), axis=1)

        print(weekly2.columns)
        print("WEEKLY ENRICHED COUNT: " + str(weekly2.shape[0]))
        print(weekly2.info)

        weekly2 = weekly2[
            (weekly2["KPIValueNew"] != weekly2["KPIValue_Compare"]) | ((weekly2["Comments_check"] != True))]
        weekly2.rename(columns={'KPINameWeekly':'KPINameQVD', 'Region':'Natco'}, inplace=True)
        weekly2['correction_timestamp'] = time.time()
        weekly_corr_result = weekly2[self.corrections.columns]
        print(weekly_corr_result.columns)
        print("WEEKLY ENRICHED COUNT: " + str(weekly_corr_result.shape[0]))
        print(weekly_corr_result.info)
        corrections_result = pd.concat([self.corrections, weekly_corr_result])


        corrections_map = corrections_result[corrections_result['Granularity'] == 'W'][['Key_Corr']].drop_duplicates()
        corrections_map['was_corrected_Flag'] = 'Correction'
        corrections_map.rename(columns={'Key_Corr':'Input_ID'}, inplace=True)
        print(corrections_map.columns)
        print("WEEKLY ENRICHED COUNT: " + str(corrections_map.shape[0]))
        print(corrections_map.info)

        output_update = pd.merge(input_raw,corrections_map, on='Input_ID', how='left')
        output_update_ids = output_update[['Input_ID']].drop_duplicates()
        output_update_ids['is_in_input'] = 1

        output_full = self.weekly_output.copy()

        output_filtered = pd.merge(output_full, output_update_ids, on="Input_ID", how="left")
        output_filtered = output_filtered[output_filtered['is_in_input'].isna()]
        output_filtered = output_filtered[self.weekly_output.columns]
        print(output_filtered.columns)
        print("WEEKLY OUTPUT FILTERED COUNT: " + str(output_filtered.shape[0]))
        print(output_filtered.info)

        output_result = pd.concat([output_update,output_filtered])

        return {"output": output_result, "corrections": corrections_result}



