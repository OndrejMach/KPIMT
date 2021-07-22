from functions import clean_input, enrich_with_kpi_values, get_new_corrections, get_update
import pandas as pd

class WeeklyProcessing:
    weekly_output=None
    raw_input=None
    corrections=None
    natco=None
    filename=None
    filename_timestamp=None


    def __init__(self, weekly_output,raw_input, corrections, natco, filename, filename_timestamp):
        self.weekly_output=weekly_output
        self.raw_input=raw_input
        self.corrections=corrections
        self.natco=natco
        self.filename=filename
        self.filename_timestamp=filename_timestamp

    def process_data(self):
        output = self.weekly_output.copy()
        output.rename(columns={"Input_ID": "Input_ID_Old", "Value": "KPIValueOld", "Remarks": "RemarksOld",
                               "Input_File": "Input_File_Old"}, inplace=True)
        output = output[["Input_ID_Old", "KPIValueOld", "RemarksOld", "Input_File_Old"]]
        input = self.raw_input.copy()

        #print(input.info)
        input_cleaned = clean_input(input=input,natco=self.natco, filename=self.filename, file_timestamp=self.filename_timestamp, period_column= "KPINameWeekly", period_id="w")

        #print(input_cleaned.info)

        enriched_input = enrich_with_kpi_values(input=input_cleaned, output=output)

        new_corrections = get_new_corrections(input=enriched_input, kpi_column_name="KPINameWeekly", granularity="W")

        #print(input_cleaned.info)
        weekly_update = get_update(new_corrections=new_corrections, input_cleaned=input_cleaned, granularity="W",
                                    kpi_column="KPINameWeekly")

        outer_join = self.weekly_output.merge(weekly_update, how='outer', indicator=True)
        anti_join = outer_join[~(outer_join._merge == 'both')].drop('_merge', axis=1)
        return pd.concat([weekly_update, anti_join])





