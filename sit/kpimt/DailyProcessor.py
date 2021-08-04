import time
from datetime import datetime
from sit.kpimt.confs import output_schema, corections_schema, output_daily_schema

import numpy as np
import pandas as pd


class DailyProcessor:
    daily_output=None
    corrections_file=None
    daily_input = None
    natco = None
    filename=None
    filename_timestamp=None
    datetime_format='%Y%m%d%H%M%S'

    def __init__(self, daily_output, corrections_file,daily_input, natco,filename,filename_timestamp,datetime_format='%Y%m%d%H%M%S' ):
        self.daily_input=daily_input
        self.corrections_file=corrections_file
        self.daily_output=daily_output
        self.natco=natco
        self.filename=filename
        self.filename_timestamp=filename_timestamp
        self.datetime_format = datetime_format

    def process_data(self):
        def get_comment(df):
            if (len(df) == 0):
                return "null"
            else:
                return df['DatumKPI'].iloc[0]

        def value_compare_set(row):
            if (pd.isna(row['KPIValueCompare'])):
                row['KPIValueCompare'] = row['KPIValue']
            return row

        def comment_file_set(row):
            if (pd.isna(row['CommentFileCompare'])):
                row['CommentFileCompare'] = row['CommentFileNew']
            return row

        def get_input_id(row):
            row['Input_ID'] = "{}-{}-{}-d".format(row['Natco'], row['KPINameDaily'], row['DatumKPI'])
            return row



        print("INPUT DATA ROW COUNT: " + str(self.daily_input.shape[0]))
        input = self.daily_input[(self.daily_input['Marker'] == 'L') & (self.daily_input['Value'].notna())].copy()
        input['DatumKPI'] = input['DatumKPI'].map(lambda x: datetime.strptime(x,self.datetime_format).strftime("%d.%m.%Y"))
        input['Natco'] = self.natco
        input['KPINameDaily'] = input['KPINameDaily'].map(lambda x: str(x).upper().strip())
        input['Input_File'] = self.filename
        input['Input_File_Time'] = self.filename_timestamp
        input['Input_ID'] = None
        input = input.apply(lambda x: get_input_id(x), axis=1)
        input['KPIValue'] = input['Value'].apply(lambda x: str(x).replace(",", "."))
        input['num_values'] = input['KPIValue'].astype(str).str.contains("[a-zA-Z]", regex=True)
        input = input[input['KPIValue'].astype(str).str.contains("[0-9\.,]", regex=True) & (input['num_values'] != True)]
        input['KPIValue'] = input['KPIValue'].apply(lambda x: float(x))
        input['Value'] = input['KPIValue']
        print("INPUT DATA ROW COUNT: "+str(input.shape[0]))

        input_cleaned = input[["Input_ID","Marker", "DatumKPI", "TimestampTo", "Natco", "SourceSystem",
                               "KPINameDaily", "Denominator", "Numerator", "KPIValue", "Input_File", "Input_File_Time"]]

        comments_raw = self.daily_input[(self.daily_input['Marker'] == 'C')].copy()
        comment = get_comment(comments_raw)

        output_raw = self.daily_output.copy()
        print(output_raw.columns)
        output = output_raw.rename(columns={"Value": "KPIValueOld", "Input_File":"Input_File_Old"})
        output = output[["Input_ID", "KPIValueOld", "Input_File_Old"]]
        output = output[pd.notna(output["Input_ID"])]

        value_map = output[['Input_ID', 'KPIValueOld']].drop_duplicates()
        kpi_value_map = value_map.rename(columns = {'KPIValueOld':'KPIValueCompare'})
        input_file_map = output[['Input_ID', 'Input_File_Old']].drop_duplicates()

        daily2 = pd.merge(input_cleaned, value_map, on="Input_ID", how="left")
        daily2 = pd.merge(daily2,kpi_value_map, on="Input_ID", how="left")
        daily2 = daily2.apply(lambda x: value_compare_set(x), axis=1)
        daily2 = pd.merge(daily2,input_file_map, on="Input_ID", how="left")
        daily2.rename(columns = {'KPIValue': 'KPIValueNew','Input_File': 'Input_File_New' }, inplace=True)
        daily2['CommentFileNew'] = comment
        daily2 = daily2[['Input_ID', 'Marker', 'DatumKPI','TimestampTo', 'Natco', 'SourceSystem', 'KPINameDaily', 'Denominator', 'Numerator', 'KPIValueNew', 'KPIValueOld', 'KPIValueCompare', 'Input_File_Old', 'Input_File_New', 'CommentFileNew', 'Input_File_Time']]

        print("ENRICHED DAILY DATA COUNT: "+ str(daily2.shape[0]))

        print("CORRECTIONS COLUMNS: "+str(self.corrections_file.columns))

        corr = self.corrections_file[['Key_Corr','DatumKPI','KPINameQVD','Natco',
                                      'KPIValueOld','KPIValueNew','CommentRowOld',
                                      'CommentRowNew','CommentFileOld','CommentFileNew','TimestampCorrFile',
                                      'Granularity','FileOld','FileNew', 'correction_timestamp']].copy()

        CommentFileOld_Map = self.corrections_file.fillna('').groupby(['Key_Corr']).agg(CommentFileOld=("CommentFileNew", np.max)).reset_index().rename(columns={'Key_Corr': 'Input_ID'}).drop_duplicates()
        CommentFileCompare_Map = CommentFileOld_Map.rename(columns={'CommentFileOld': 'CommentFileCompare'})



        Update_Corr = pd.merge(daily2,CommentFileOld_Map, on="Input_ID", how="left")
        Update_Corr = pd.merge(Update_Corr,CommentFileCompare_Map, on="Input_ID", how="left" )
        Update_Corr = Update_Corr.apply(lambda x: comment_file_set(x), axis=1 )
        Update_Corr.rename(columns={'Input_ID': 'Key_Corr', 'KPINameDaily' : 'KPINameQVD', 'Input_File_Time':'TimestampCorrFile', 'Input_File_Old':'FileOld', 'Input_File_New': 'FileNew'}, inplace=True)
        Update_Corr['Granularity'] = 'D'
        Update_Corr['CommentRowOld'] = None
        Update_Corr['CommentRowNew'] = None
        Update_Corr['correction_timestamp'] = time.time()
        #Update_Corr = Update_Corr[['Key_Corr','DatumKPI','KPINameQVD','Natco','KPIValueOld','KPIValueNew','KPIValueCompare','CommentRowOld','CommentRowNew','CommentFileOld','CommentFileCompare','TimestampCorrFile','FileOld','FileNew','Granularity']]
        Update_Corr = Update_Corr[Update_Corr['KPIValueNew'] != Update_Corr['KPIValueCompare']]
        Update_Corr = Update_Corr[corr.columns]
        correction_result = pd.concat([corr,Update_Corr])[corections_schema]

        print("NEW CORRECTION FILE ROW COUNT: "+str(correction_result.shape[0]))
        correction_map = correction_result.copy()
        print(correction_map.columns)
        correction_map['was_corrected_Flag'] = 'Correction'
        correction_map = correction_map[['Key_Corr', 'was_corrected_Flag']].drop_duplicates()
        correction_map.rename(columns={'Key_Corr':'Input_ID'}, inplace=True)
        result_update = pd.merge(input_cleaned, correction_map, on="Input_ID", how="left")
        result_update = result_update.rename(columns={'DatumKPI':'Date','Natco':'Region','KPINameDaily': 'KPI_ID','KPIValue':'Value'})
        result_update = result_update[['Input_ID','Date','TimestampTo','Region','SourceSystem','KPI_ID','Denominator','Numerator','Value','Input_File','was_corrected_Flag']]

        update_ids= result_update[['Input_ID']].copy()
        update_ids['present_in_daily'] = 1
        update_ids = update_ids.drop_duplicates()
        print(update_ids.columns)
        print(output_raw.columns)
        output_filtered = pd.merge(output_raw,update_ids, on="Input_ID", how="left")
        print(output_filtered.columns)
        output_filtered = output_filtered[pd.isna(output_filtered['present_in_daily'])]

        output_filtered=output_filtered[['Input_ID','Date','TimestampTo','Region','SourceSystem','KPI_ID','Denominator','Numerator','Value','Input_File','was_corrected_Flag']]

        output = pd.concat([result_update,output_filtered])[output_daily_schema]
        print("OUTPUT COUNT: "+str(output.shape[0]))
        return {"output": output, "corrections": correction_result}






















