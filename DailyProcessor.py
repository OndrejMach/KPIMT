import pandas as pd

from functions import get_date, get_new_corrections_daily


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

        def update_kpi_value(row):
            if pd.isna(row['KPIValue_Old']) :
                row['KPIValue_Compare'] = row['KPIValue_New']
            else:
                row['KPIValue_Compare'] = row['KPIValue_Old']
            return row

        input = self.daily_input[(self.daily_input['Marker'] == 'L') & (self.daily_input['KPIValue'].notna())].copy()
        input['DatumKPI'] = input['DatumKPI'].map(lambda x: get_date(x))
        input['Natco'] = self.natco
        input['KPINameDaily'] = input['KPINameDaily'].map(lambda x: str(x).upper().strip())
        input['Input_File'] = self.filename
        input['Input_File_Time'] = self.filename_timestamp
        input['Input_ID'] = input.apply(lambda x: "{}-{}-{}-d".format(x['Natco'], x['KPINameDaily'], x['DatumKPI']), axis=1)
        print(input.info)

        input_cleaned = input[["Input_ID","Marker", "DatumKPI", "TimestampTo", "Natco", "SourceSystem",
                               "KPINameDaily", "Denominator", "Numerator", "KPIValue", "Input_File", "Input_File_Time"]]

        comments_raw = self.daily_input[(self.daily_input['Marker'] == 'L') & (self.daily_input['KPIValue'].notna())].copy()
        comment = get_comment(comments_raw)

        output_raw = self.daily_output.copy()
        output = output_raw.rename(columns={"Value": "KPIValue_Old", "Input_File":"Input_File_Old"})
        output = output[["Input_ID", "KPIValue_Old", "Input_File_Old"]]

        input_to_enrich = input_cleaned.rename(columns={"KPIValue" : "KPIValue_New", "Input_File": "Input_File_New"})
        input_to_enrich['CommentFileNew'] = comment
        input_to_enrich['Input_File_Time'] = self.filename_timestamp
        input_enriched = pd.merge(input_to_enrich, output, left_on="Input_ID", right_on="Input_ID")
        input_enriched['KPIValue_Compare'] = None
        input_enriched = input_enriched.apply(lambda row: update_kpi_value(row), axis=1)

        corrections_file = self.corrections_file.sort_values(by=["Key_Corr","CommentFileNew" ])
        latest_corrections = corrections_file.groupby(["Key_Corr"]).agg(CommentFileOld=("CommentFileNew", "last")).reset_index()
        latest_corrections.rename(columns={"Key_Corr":"Key_Corr_Old"}, inplace=True)
        input_enriched.rename(columns={"Input_ID": "Key_Corr"}, inplace=True)
        print(latest_corrections.info)
        print(input_enriched.info)

        correction_update = pd.merge(input_enriched,latest_corrections, left_on="Key_Corr", right_on="Key_Corr_Old" )
        correction_update['CommentFile_Compare'] = None
        correction_update = correction_update.apply(lambda row: update_corrections(row), axis=1)

        new_corrections = get_new_corrections_daily(correction_update)
        all_corrections = pd.concat([new_corrections, self.corrections_file])

        input_cleaned.rename(columns={"DatumKPI":"Date", "Natco": "Region", "KPINameDaily": "KPI_ID","KPIValue":"Value"}, inplace=True)
        daily_update = pd.merge(input_cleaned,all_corrections, left_on="Input_ID", right_on="Key_Corr" )
        daily_update['was_corrected_Flag'] =""
        daily_update = daily_update.apply(lambda row: was_corrected(row), axis=1)
        print(daily_update.info)

        daily_update = daily_update[["Input_ID","Date", "TimestampTo", "Region", "SourceSystem",
        "KPI_ID", "Denominator", "Numerator", "Value", "Input_File", "was_corrected_Flag"]]

        outer_join = self.daily_output.merge(daily_update, how='outer', indicator=True)
        anti_join = outer_join[~(outer_join._merge == 'both')].drop('_merge', axis = 1)
        return pd.concat([daily_update,anti_join])





















