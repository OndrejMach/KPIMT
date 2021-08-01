import re
from datetime import datetime
from os import listdir, stat
from os.path import isfile, join

import pandas as pd

from sit.kpimt.confs import paths
from sit.kpimt.structures import daily_raw_header


def get_files(path):
    files= [f for f in listdir(path) if isfile(join(path, f))]
    print(files)
    return sorted(files, key=lambda t: stat(path+t).st_mtime)

def get_path(basepath,natco, mode):
    return "{}/{}/{}/".format(basepath,natco,paths[mode])

def get_file_timestamp(file_path):
    fs = stat(file_path)
    print("getting timestamp for a file: "+file_path)
    timestamp = fs.st_mtime
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d-%H:%M:%S')

def get_input_data(file):
    if (str(file).endswith(".csv")):
        return pd.read_csv(file,delimiter='|', header=None, names=daily_raw_header)
    if (str(file).endswith("xls") or str(file).endswith("xlsx")):
        return pd.read_excel(file, header=0)

def date_to_month(date):
    return "00{}{}".format(date[4:6], date[0:4])


def get_date(date):
    m = re.search("(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})", date)
    if (m is not None):
        return "{}.{}.{}".format(m.group(3), m.group(2), m.group(1))
    else:
        return "Unknown Format"


def trunc_date(date):
    m = re.search("(\d+)[\./\-](\d+)[\./\-](\d+)", date)
    if (m is not None):
        return "01.{}.{}".format(m.group(2), m.group(3))
    else:
        return date


def update_kpi_name(row):
    if (pd.isna(row['KPI_name']) or len(row['KPI_name']) == 0):
        row['KPI_name'] = "*nonamefor_" + row['KPI_ID']
    return row


def format_date(date):
    m = re.search("(\d+)[\./\-](\d+)[\./\-](\d+)", date)
    if (m is not None):
        return "{}.{}.{}".format(m.group(1), m.group(2), m.group(3))
    else:
        return "Unknown Format"


def clean_input(input, natco, filename, file_timestamp, period_column, period_id):
    input["DatumKPI"] = input["Date"].map(lambda x: format_date(str(x)))
    input["DatumKPI"] = input["DatumKPI"].map(lambda x: trunc_date(str(x)))
    input[period_column] = input["KPI name"].map(lambda x: str(x).upper().strip())
    input["Region"] = natco
    input.rename(columns={"Value": "KPIValue"}, inplace=True)
    input["Input_File"] = filename
    input["Input_File_Time"] = file_timestamp
    input['Input_ID'] = input.apply(lambda x: "{}-{}-{}-{}".format(x['Region'], x['KPI name'], x['Date'], period_id),
                                    axis=1)
    input["Remarks"] = input["Remarks"].map(lambda x: re.sub(r"[^A-Za-z0-9_ \n]","?" ,str(x).strip()))

    return input[["Input_ID", "DatumKPI", period_column, "Region", "KPIValue", "Remarks", "Input_File",
        "Input_File_Time"]]


def update_kpi_value(row):
    if (pd.isna(row['KPIValueOld']) or len(row['KPIValueOld']) == 0):
        row['KPIValue_Compare'] = row['KPIValueNew']
    else:
        row['KPIValue_Compare'] = row['KPIValueOld']
    return row


def enrich_with_kpi_values(input, output):
    def update_remarks(row):
        if (pd.isna(row['RemarksOld']) or len(row['RemarksOld']) == 0):
            row['Remarks_Compare'] = row['Remarks']
        else:
            row['Remarks_Compare'] = row['RemarksOld']



    input_renamed = input.rename(columns={"KPIValue": "KPIValueNew"})

    enriched_input = pd.merge(input_renamed, output, left_on="Input_ID", right_on="Input_ID_Old")
    enriched_input["KPIValue_Compare"] = None
    enriched_input["Remarks_Compare"] = None
    enriched_input.apply(lambda row: update_kpi_value(row), axis=1)
    enriched_input.apply(lambda row: update_remarks(row), axis=1)
    return enriched_input

def kpi_updated(row):
    if (str(row['KPIValueNew']).strip() == str(row['KPIValueCompare']).strip().upper()):
        row['hasUpdate'] = False
    else:
        row['hasUpdate'] = True

def get_new_corrections_daily(input):
    now = datetime.now().time()

    corrections = input.copy()
    current_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")
    corrections["CommentRowOld"] = ""
    corrections["CommentRowNew"] = ""
    corrections["Granularity"] = "D"
    corrections["correction_timestamp"] = current_time
    corrections.rename(columns={"KPINameDaily": "KPINameQVD", "Input_File_Time": "TimestampCorrFile", "Input_File_Old": "FileOld",
                                "Input_File_New": "FileNew", "KPIValue_Old": "KPIValueOld",
                                "KPIValue_New": "KPIValueNew", "KPIValue_Compare": "KPIValueCompare",
                                "Input_File": "FileNew"}, inplace=True)
    print(corrections.columns)

    corrections["hasUpdate"] = False
    corrections.apply(lambda row: kpi_updated(row), axis = 1)

    new_corrections = corrections[corrections['hasUpdate'] == True]
    return new_corrections[["Key_Corr", "DatumKPI", "KPINameQVD", "Natco", "KPIValueOld", "KPIValueNew",
                           "CommentRowOld", "CommentRowNew", "CommentFileOld", "CommentFileNew",
                           "TimestampCorrFile", "FileOld", "FileNew", "Granularity", "correction_timestamp"]]


def get_new_corrections(input, kpi_column_name, granularity):
    now = datetime.now().time()

    corrections = input.copy()
    current_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")
    corrections.rename(columns={"Input_ID": "Key_Corr", kpi_column_name: "KPINameQVD", "Region": "Natco",
                                "RemarksOld": "CommentRowOld", "Remarks": "CommentRowNew",
                                "Input_File_Time": "TimestampCorrFile", "Input_File_Old": "FileOld",
                                "Input_File": "FileNew"}, inplace=True)
    corrections["CommentFileOld"] = ""
    corrections["CommentFileNew"] = ""
    corrections["Granularity"] = granularity
    corrections["correction_timestamp"] = current_time
    corrections["hasUpdate"] = False
    corrections.apply(lambda row: kpi_updated(row))

    new_corrections = corrections[corrections['hasUpdate'] == True]
    return new_corrections[["Key_Corr", "DatumKPI", "KPINameQVD", "Natco", "KPIValueNew", "KPIValueOld",
                           "CommentRowOld", "CommentRowNew", "CommentFileOld", "CommentFileNew",
                           "TimestampCorrFile", "Granularity", "FileOld", "FileNew", "correction_timestamp"]]


def get_update(new_corrections, input_cleaned, kpi_column, granularity):
    corrections_tmp = new_corrections[new_corrections['Granularity'] == granularity].copy()
    corrections_tmp['was_corrected_Flag'] = "Correction"
    corrections_map = corrections_tmp[["Key_Corr", "was_corrected_Flag"]]

    monthly_update_tmp = input_cleaned.copy()
    monthly_update_tmp.rename(columns={"DatumKPI": "Date", kpi_column: "KPI name", "KPIValue": "Value"},inplace=True)
    #print(monthly_update_tmp.info)

    monthly_update_join = pd.merge(monthly_update_tmp, corrections_map, left_on="Input_ID", right_on="Key_Corr")
    monthly_update = monthly_update_join[
        ["Input_ID", "Date", "KPI name", "Region", "Value", "Remarks", "Input_File",
            "was_corrected_Flag"]]
    monthly_update.drop_duplicates(subset=['KPI name', 'Date'])
    return monthly_update

def number_shaver(ch,
                  regx = re.compile('(?<![\d.])0*(?:'
                                    '(\d+)\.?|\.(0)'
                                    '|(\.\d+?)|(\d+\.\d+?)'
                                    ')0*(?![\d.])')  ,
                  repl = lambda mat: mat.group(mat.lastindex)
                                     if mat.lastindex!=3
                                     else '0' + mat.group(3) ):

    ret = regx.sub(repl,ch)
    if (pd.isna(ret) or ret == 'nan'):
        ret = ''
    return ret
