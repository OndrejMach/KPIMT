import pandas as pd

from sit.kpimt.DailyProcessor import DailyProcessor
from sit.kpimt.Weekly_input import Weekly_input
from sit.kpimt.Monthly_input import Monthly_input
from sit.kpimt.confs import outputs
from sit.kpimt.functions import get_path, get_files, get_input_data, get_file_timestamp
import os


class Output_processor:

    def run_input_processing(self ,natco, mode, basepath, period, output_path, corrections_path):
        path = get_path(basepath, natco, mode)
        files_to_process = get_files(path)
        for input in files_to_process:
            print("processing file: " + input)
            output_file = '{}/{}{}'.format(output_path, natco, outputs[mode])
            corrections_file = '{}/{}'.format(corrections_path, "Corrections.csv")
            corrections = pd.read_csv(corrections_file, delimiter='|', header=0, dtype=str)
            input_data = get_input_data(path + input)
            # print(input_data.info)
            output = pd.read_csv(output_file, delimiter='|', header=0, dtype=str)
            timestamp_str = get_file_timestamp(path + input)
            print(timestamp_str)
            if (period == 'daily'):
                processor = DailyProcessor(daily_output=output, daily_input=input_data,
                                           corrections_file=corrections, natco=natco,
                                           filename=input, filename_timestamp=timestamp_str)
                daily_proc = processor.process_data()
                daily_proc["output"].to_csv(output_file, sep="|", index=False)
                daily_proc["corrections"].to_csv(corrections_file, sep="|", index=False)

            elif (period == 'weekly'):

                processor = Weekly_input(weekly_output=output, raw_input=input_data, corrections=corrections,
                                         natco=natco, filename=input, filename_timestamp=timestamp_str)
                daily_proc = processor.process_data()
                daily_proc["output"].to_csv(output_file, sep="|", index=False)
                daily_proc["corrections"].to_csv(corrections_file, sep="|", index=False)
            else:
                processor = Monthly_input(monthly_output=output, raw_input=input_data, corrections=corrections,
                                          natco=natco, filename=input, filename_timestamp=timestamp_str)
                daily_proc = processor.process_data()
                daily_proc["output"].to_csv(output_file, sep="|", index=False)
                daily_proc["corrections"].to_csv(corrections_file, sep="|", index=False)
            os.remove(path + input)
        return len(files_to_process)
