import re
from datetime import datetime
from os import listdir, stat
from os.path import isfile, join

import pandas as pd

from sit.kpimt.confs import paths
from sit.kpimt.structures import daily_raw_header


def get_files(path):
    files= [f for f in listdir(path) if (isfile(join(path, f)) and (f.endswith(".csv") or f.endswith(".xls") or f.endswith(".xlsx")))]
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
        if ("daily" in str(file)):
            return pd.read_csv(file,delimiter='|', header=None, names=daily_raw_header)
        elif ("monthly" in str(file)):
            return pd.read_csv(file, delimiter='|', header=0)
    if (str(file).endswith("xls") or str(file).endswith("xlsx")):
        xl = pd.ExcelFile(file)
        sheet= "Sheet1"
        if ("Sheet1" in xl.sheet_names):
            sheet = "Sheet1"
        elif ("Sheet1$" in xl.sheet_names):
            sheet = "Sheet1$"
        elif ("sheet1" in xl.sheet_names):
            sheet = "sheet1"

        return pd.read_excel(file, header=0, sheet_name=sheet)

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



def update_kpi_value(row):
    if (pd.isna(row['KPIValueOld']) or len(row['KPIValueOld']) == 0):
        row['KPIValue_Compare'] = row['KPIValueNew']
    else:
        row['KPIValue_Compare'] = row['KPIValueOld']
    return row



def kpi_updated(row):
    if (str(row['KPIValueNew']).strip() == str(row['KPIValueCompare']).strip().upper()):
        row['hasUpdate'] = False
    else:
        row['hasUpdate'] = True

def isfloat(value):
  try:
    float(value)
    return True
  except ValueError:
    return False

