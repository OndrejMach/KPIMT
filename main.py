import pandas as pd
import numpy as np
from structures import daily_header, daily_raw_header
from functions import format_date
from KPIs_reader import KPI_reader
from DailyProcessor import DailyProcessor
from WeeklyProcessing import WeeklyProcessing
from MonthlyInputProcessing import MonthlyInputsProcessor
from datetime import datetime

import argparse

from os import listdir, stat
from os.path import isfile, join


natcos=["TMA", "TMCZ", "COSGRE", "TMHR", "COSROM", "GLOBUL", "TMD", "TMCG", "TMHU", "TMMK", "TMNL", "TMPL", "TMSK", "AMC"]
modes=["daily_input", "weekly_input", "monthly_input", "weekly_update", "monthly_update", "matrix"]
paths={"daily_input":"daily_DWH_feed", "weekly_input":"weekly_input", "monthly_input":"monthly_input", "weekly_update":"daily_DWH_feed", "monthly_update":"daily_DWH_feed", "matrix": "...TODO.."}
outputs={"daily_input":"_daily.csv", "weekly_input":"_weekly.csv", "monthly_input": "_monthly.csv"}

kpis_path="/Users/ondrejmachacek/tmp/KPI/new_input/"
correnctions_path="/Users/ondrejmachacek/tmp/KPI/new_input/Corrections.csv"
basepath=""
output_path="/Users/ondrejmachacek/tmp/KPI/new_input/"

parser = argparse.ArgumentParser()
parser.add_argument('--natco', help='specify what NatCo to process (now supported: {})'.format(natcos), required=True, choices=natcos)
parser.add_argument('--mode', help='mode or operation (suported: {})'.format(modes), required=True, choices=modes)
args = parser.parse_args()


def get_files(path):
    files= [f for f in listdir(path) if isfile(join(path, f))]
    return sorted(files, key=lambda t: stat(path+t).st_mtime)

def get_path(basepath,natco, mode):
    return "{}/{}/{}/".format(basepath,natco,paths[mode])

def get_file_timestamp(file_path):
    fs = stat(file_path + input)
    timestamp = fs.st_mtime
    return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d-%H:%M:%S')

def get_input_data(file):
    if (str(file).endswith(".csv")):
        return pd.read_csv(file,delimiter='|', header=None, names=daily_raw_header)
    if (str(file).endswith("xls") or str(file).endswith("xlsx")):
        return pd.read_excel(file, header=0)

def run_input_processing(natco,mode,basepath,corrections, period):
    path = get_path(basepath,natco,mode)
    files_to_process = get_files(path)
    output_file ='{}/{}{}'.format(output_path,natco, outputs[mode])
    for input in files_to_process:
        input_data = get_input_data(path+input)
        output = pd.read_csv(output_file,delimiter='|', header=0)
        timestamp_str =get_file_timestamp(path+input)
        if (period == 'daily'):
            processor = DailyProcessor(daily_output=output, daily_input=input_data,
                                    corrections_file=corrections,natco=natco,
                                    filename=input, filename_timestamp=timestamp_str )
        elif (period == 'weekly')
            processor = WeeklyProcessing(weekly_output=output, raw_input=input_data, corrections=corrections,
                                   natco=natco, filename=input,filename_timestamp=timestamp_str)
        else:
            processor = MonthlyInputsProcessor(monthly_output=output, raw_input=input_data,
                                          corrections=corrections, natco=natco,
                                          filename=input, timestamp=timestamp_str)
        daily_proc = processor.process_data()
        daily_proc.to_csv(output_file+"_tmp")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    print(pd.__version__)
    kpis = KPI_reader(kpis_path).read_data()
    corrections = pd.read_csv(correnctions_path,delimiter='|', header=0)
    #print(format_date("01/02/1984"))

    #print(args.natco)
    if (args.mode == 'daily_input'):
        run_input_processing(natco=args.natco,mode=args.mode,basepath=basepath, corrections=corrections,
                             period="daily")
    elif (args.mode == 'weekly_input'):
        run_input_processing(natco=args.natco, mode=args.mode, basepath=basepath, corrections=corrections,
                             period="weekly")
    elif (args.mode == 'monthly_input'):
        run_input_processing(natco=args.natco, mode=args.mode, basepath=basepath, corrections=corrections,
                             period="monthly")



# See PyCharm help at https://www.jetbrains.com/help/pycharm/
