import pandas as pd


class Multimarket:
    iso_mapping = {"F1":"F2", "AT" : "TMA", "RO": "COSROM", "CZ":"TMCZ", "HR" : "TMHR"}
    all_monthly = None
    multimarket_in = None
    filename = None
    filetime = None

    def __init__(self, all_monthly, multimarket_in, filename, filetime):
        self.all_monthly = all_monthly
        self.multimarket_in = multimarket_in
        self.filetime = filetime
        self.filename = filename


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


        input = self.multimarket_in[self.multimarket_in['Value'].notna()].copy()
        input['KPINameMonthly'] = input['KPI name'].apply(lambda x: str(x).upper().strip())
        input['Region'] = input['Region'].apply(lambda x: self.iso_mapping[str(x)])
        input['KPIValue'] = input['Value'].apply(lambda x: float(x))
        input['Input_File'] =self.filename
        input['Input_File_Time'] = self.filetime
        input['Input_ID'] = None
        input = input.apply(lambda row: get_input_id(row), axis = 1)
        input = input[['Input_ID','DatumKPI', 'KPINameMonthly', 'Region', 'KPIValue', 'Remarks', 'Input_File', 'Input_File_Time' ]]
        input['KPIValueNew'] = input['KPIValue']

        monthly = self.all_monthly[self.all_monthly['Input_ID'].notna()].copy()
        monthly.rename(columns={"Value", "KPIValueOld"}, inplace=True)
        value_map = monthly[['Input_ID', 'KPIValueOld']].drop_duplicates()
        input_file_map = monthly[['Input_ID', 'Input_File']].drop_duplicates()
        input_file_map.rename(columns={"Input_File" : "Input_File_Old"}, inplace=True)
        remarks_map=monthly[['Input_ID', 'Remarks']].drop_duplicates()
        remarks_map.rename(columns={"Remarks" : "Remarks_Old"})

        input2 = pd.merge(input,value_map, how="left", on="Input_ID" )
        input2['KPIValue_Compare'] = input2['KPIValueOld']
        input2 = input2.apply(lambda row: evaluate_value(row), axis=1)
        input2 = pd.merge(input2,input_file_map, how="left", on="Input_ID" )
        input2 = pd.merge(input2,remarks_map,how="left", on="Input_ID" )
        input2['Remarks_Compare'] = input2['Remarks_Old']
        input2 = input2.apply(lambda row: evaluate_remarks(row), axis=1)

        input2['remarks_check'] = input2.apply(lambda x: str(x['Remarks']) in str(x['Remarks_Compare']),
                                                    axis=1)

        update_corr = input2[input2['KPIValueNew'] != input2['KPIValue_Compare'] | (input2['remarks_check'] != True)]\
            .rename(columns = {"Input_ID":"Key_Corr",
                               "Remarks_Old": "CommentRowOld",
                               "Remarks": "CommentRowNew",
                               "Input_File_Time": "TimestampCorrFile",
                               "Input_File_Old": "FileOld",
                               "Input_File" : "FileNew"
                               })
        update_corr['Granularity'] = 'M'








