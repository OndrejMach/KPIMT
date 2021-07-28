import pandas as pd
import numpy as np
import time

from functions import get_date, get_new_corrections_daily, number_shaver


class DailyProcessor:
    daily_output=None
    corrections_file=None
    daily_input = None
    natco = None
    filename=None
    filename_timestamp=None

    def __init__(self, daily_output, corrections_file,daily_input, natco,filename,filename_timestamp ):
        self.daily_input=daily_input
        self.corrections_file=corrections_file
        self.daily_output=daily_output
        self.natco=natco
        self.filename=filename
        self.filename_timestamp=filename_timestamp

    def process_data(self):
        def get_comment(df):
            if (len(df) == 0):
                return "null"
            else:
                return df['DatumKPI'].iloc[0]

        def update_corrections(row):
            if (pd.isna(row['CommentFileOld']) or len(row['CommentFileOld'])==0):
                row['CommentFile_Compare'] = row['CommentFileNew']
                return row
            else:
                row['CommentFile_Compare'] = row['CommentFileOld']
                return row

        def was_corrected(row):
            if (pd.isna(row['Key_Corr'])):
                row['was_corrected_Flag'] = ""
            else:
                row['was_corrected_Flag'] = "Correction"
            return row

        def update_kpi_value(row):
            if pd.isna(row['KPIValue_Old']) :
                row['KPIValue_Compare'] = row['KPIValue_New']
            else:
                row['KPIValue_Compare'] = row['KPIValue_Old']
            return row

        def value_compare_set(row):
            if (pd.isna(row['KPIValueCompare'])):
                row['KPIValueCompare'] = row['KPIValue']
            return row

        def comment_file_set(row):
            if (pd.isna(row['CommentFileCompare'])):
                row['CommentFileCompare'] = row['CommentFileNew']
            return row


        input = self.daily_input[(self.daily_input['Marker'] == 'L') & (self.daily_input['KPIValue'].notna())].copy()
        input['DatumKPI'] = input['DatumKPI'].map(lambda x: get_date(x))
        input['Natco'] = self.natco
        input['KPINameDaily'] = input['KPINameDaily'].map(lambda x: str(x).upper().strip())
        input['Input_File'] = self.filename
        input['Input_File_Time'] = self.filename_timestamp
        input['Input_ID'] = input.apply(lambda x: "{}-{}-{}-d".format(x['Natco'], x['KPINameDaily'], x['DatumKPI']), axis=1)
        input['KPIValue'] = input['KPIValue'].apply(lambda x: str(x).replace(",", "."))
        input = input[input['KPIValue'].str.contains("[0-9\.,]", regex=True)]
        input['KPIValue'] = input['Value'].apply(lambda x: float(x))
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
                                      'CommentRowNew','CommentFileOld','TimestampCorrFile',
                                      'FileOld','FileNew','Granularity', 'correction_timestamp']].copy()

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
        Update_Corr = Update_Corr[['Key_Corr','DatumKPI','KPINameQVD','Natco',
                                      'KPIValueOld','KPIValueNew','CommentRowOld',
                                      'CommentRowNew','CommentFileOld','TimestampCorrFile',
                                      'FileOld','FileNew','Granularity', 'correction_timestamp']]
        correction_result = pd.concat([corr,Update_Corr])

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

        output = pd.concat([result_update,output_filtered])
        print("OUTPUT COUNT: "+str(output.shape[0]))
        return {"output": output, "corrections": correction_result}


        #NewestCommentsPerKPI.to_csv("/Users/ondrejmachacek/tmp/KPI/output/blbost.txt")










        # input_to_enrich = input_cleaned.rename(columns={"KPIValue" : "KPIValue_New", "Input_File": "Input_File_New"})
        # input_to_enrich['CommentFileNew'] = comment
        # input_to_enrich['Input_File_Time'] = self.filename_timestamp
        # input_enriched = pd.merge(input_to_enrich, output, left_on="Input_ID", right_on="Input_ID", how="left")
        # input_enriched['KPIValue_Compare'] = None
        # input_enriched = input_enriched.apply(lambda row: update_kpi_value(row), axis=1)
        #
        # print("INPUT CLEANED: "+str(input_cleaned.shape[0]))
        # print("INPUT ENRICHED: " + str(input_enriched.shape[0]))
        #
        # corrections_file = self.corrections_file.sort_values(by=["Key_Corr","CommentFileNew" ])
        # latest_corrections = corrections_file.groupby(["Key_Corr"]).agg(CommentFileOld=("CommentFileNew", "last")).reset_index()
        # latest_corrections.rename(columns={"Key_Corr":"Key_Corr_Old"}, inplace=True)
        # input_enriched.rename(columns={"Input_ID": "Key_Corr"}, inplace=True)
        #
        # #print(latest_corrections.info)
        # #print(input_enriched.info)
        #
        # correction_update = pd.merge(input_enriched,latest_corrections, left_on="Key_Corr", right_on="Key_Corr_Old", how="left" )
        # correction_update['CommentFile_Compare'] = None
        # correction_update = correction_update.apply(lambda row: update_corrections(row), axis=1)
        #
        # new_corrections = get_new_corrections_daily(correction_update)
        # all_corrections = pd.concat([new_corrections, self.corrections_file])
        #
        # print(all_corrections.columns)
        #
        # print("ALL_CORRECTIONS: " + str(all_corrections.shape[0]))
        #
        # input_cleaned.rename(columns={"DatumKPI":"Date", "Natco": "Region", "KPINameDaily": "KPI_ID","KPIValue":"Value"}, inplace=True)
        # daily_update = pd.merge(input_cleaned,all_corrections, left_on="Input_ID", right_on="Key_Corr", how="left" )
        # daily_update['was_corrected_Flag'] =""
        # print(daily_update.columns)
        # daily_update = daily_update.apply(lambda row: was_corrected(row), axis=1)
        # print(daily_update.columns)
        #
        # daily_update = daily_update[["Input_ID","Date", "TimestampTo", "Region", "SourceSystem", "KPI_ID", "Denominator", "Numerator", "Value", "Input_File", "was_corrected_Flag"]].astype(str)
        #
        # print("DAILY_UPDATE: " + str(all_corrections.shape[0]))
        # print(self.daily_output.columns)
        # print(daily_update.columns)
        #
        #
        # outer_join = self.daily_output.merge(daily_update, how='outer', indicator=True)
        # anti_join = outer_join[~(outer_join._merge == 'both')].drop('_merge', axis = 1)
        # all = pd.concat([daily_update,anti_join])
        # #all['Value'] = all['Value'].apply(lambda x: number_shaver(str(x)))
        # #all['Denominator'] = all['Denominator'].apply(lambda x: number_shaver(str(x)))
        # #all['Numerator'] = all['Numerator'].apply(lambda x: number_shaver(str(x)))
        # all['Value'] = all['Value'].fillna('')
        # all['Denominator'] = all['Denominator'].fillna('')
        # all['Numerator'] = all['Numerator'].fillna('')

        #print(daily_update.info)
       # return {"output" : all, "corrections": all_corrections}





















