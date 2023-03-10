import pandas as pd
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from functools import reduce

iso_mapping = {
    "F1":"F2",
    "AT":"TMA",
    "RO":"COSROM",
    "CZ":"TMCZ",
    "HR":"TMHR",
    "DE":"TMD",
    "HU":"TMHU",
    "AL":"AMC",
    "GR":"COSGRE",
    "CG":"TMCG",
    "MK":"TMMK",
    "NL":"TMNL",
    "PL":"TMPL",
    "SK":"TMSK",
    "UK" : "NO_NATCO"
}

def set_requested(row):
    if ( not pd.isnull(row['Requested'])):
        if ("1" in str(row['Natco'])):
            row['requested_Weekly'] = int(1)
        elif ("2" in str(row['Natco'])):
            row['requested_Monthly'] = int(1)
        else:
            row['requested_Daily'] = int(1)
    row['KPI_ID'] = str(row['KPI_ID']).strip()
    row['KPI_Name'] = str(row['KPI_Name']).strip()
    row['Natco'] = str(row['Natco'])[0:2]
    return row

def check_year(year):
    edate = date.today()
    year_now = edate.year
    if (year_now == year):
        return 1
    else:
        return 0


def check_last_year(year):
    edate = date.today() - relativedelta(years=1)
    year_now = edate.year
    if (year_now == year):
        return True
    else:
        return False

def check_last_ytd(date):
    yesterday = date - timedelta(days=1)
    edate = date.today()
    year_now = edate.year
    if (yesterday.year == year_now):
        return 1
    else:
        return 0

def week_year(date):
    week = date.isocalendar()[1]
    if (date.month == 12):
        return str(date.year-1) + "-cw" + str(week)
    else:
        return str(date.year) + "-cw" + str(week)

def get_key(row, period):
    #applymap('ISO_map',Natco)  & '-' & upper(KPI_ID) & '-' & Date & '-d'  as KEY1;
    row['KEY1'] = "{}-{}-{}-{}".format(iso_mapping[row['Natco']],str(row['KPI_ID']).upper(), row['Date'].strftime("%d.%m.%Y"), period)
    return row

def get_key_w_m(row, period):
    #applymap('ISO_map',Natco)  & '-' & upper(KPI_ID) & '-' & Date & '-d'  as KEY1;
    row['KEY1'] = "{}-{}-{}-{}".format(iso_mapping[row['Natco']],str(row['KPI_Name']).upper(), row['Date'].strftime("%d.%m.%Y"), period)
    return row

def get_lookups(out):
    NATVALUEMAP = out[["Input_ID", "Value"]].groupby(['Input_ID']).agg(Value=("Value", 'max')).reset_index()
    NATTIMEMAP = out[["Input_ID", "Time"]].drop_duplicates()
    NATDELMAP = out[["Input_ID", "Value"]].groupby(['Input_ID']).agg(Value=("Value", 'max')).reset_index()
    NATDELMAP['isDelivered'] = NATDELMAP['Value'].apply(lambda x: "0" if (pd.isnull(x)) else "1")
    NATDELMAP = NATDELMAP[['Input_ID', 'isDelivered']]
    Input_FileMAP = out[["Input_ID", "Input_File"]].drop_duplicates()
    WasCorrectedMAP = out[["Input_ID", "was_corrected_Flag"]].drop_duplicates()
    return [NATVALUEMAP,NATTIMEMAP,NATDELMAP,Input_FileMAP,WasCorrectedMAP]

def all_join(orig,tables):
    print("JOINING ORIGINAL TABLE "+str(orig.columns))
    for i in tables:
        print("JOINING TABLE: "+str(i.columns))
        orig = pd.merge(orig,i, on=['Input_ID'], how='left')
    #result = reduce(lambda left, right: pd.merge(left, right, on=['Input_ID'], how='outer'), tables)
    if ('isDelivered' in orig.columns):
        orig['isDelivered'].fillna('0')
    if ('Denominator' in orig.columns):
        orig['Denominator'].fillna('0')
    return orig