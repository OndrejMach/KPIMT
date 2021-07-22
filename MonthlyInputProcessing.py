from functions import clean_input, enrich_with_kpi_values, get_new_corrections, get_update
import pandas as pd

class MonthlyInputsProcessor:
    monthly_output=None
    raw_input=None
    corrections=None
    natco=None
    filename=None
    file_timestamp=None


    def __init__(self, monthly_output, raw_input, corrections, natco, filename,timestamp):
        self.raw_input=raw_input
        self.monthly_output=monthly_output
        self.corrections=corrections
        self.natco=natco
        self.filename = filename
        self.file_timestamp = timestamp

    def process_data(self):

        output = self.monthly_output.copy()
        output.rename(columns = {"Input_ID": "Input_ID_Old", "Value" : "KPIValueOld", "Remarks":"RemarksOld","Input_File":"Input_File_Old"}, inplace=True)
        output = output[["Input_ID_Old","KPIValueOld", "RemarksOld", "Input_File_Old"]]

        input = self.raw_input[self.raw_input["Value"].notna()].copy()

        input_cleaned = clean_input(input=input,natco=self.natco, filename=self.filename, file_timestamp=self.file_timestamp, period_column="KPINameMonthly", period_id="m")

        enriched_input = enrich_with_kpi_values(input=input_cleaned, output=output)

        new_corrections = get_new_corrections(input=enriched_input, granularity="M", kpi_column_name="KPINameMonthly")

        monthly_update = get_update(new_corrections=new_corrections, input_cleaned=input_cleaned, granularity="M", kpi_column="KPINameMonthly")

        outer_join = self.monthly_output.merge(monthly_update, how='outer', indicator=True)
        anti_join = outer_join[~(outer_join._merge == 'both')].drop('_merge', axis=1)
        return pd.concat([monthly_update, anti_join])














