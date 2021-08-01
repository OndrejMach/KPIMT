import math

import pandas as pd
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from functools import reduce
natCo = 'AT'

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
    "SK":"TMSK"
}

def set_requested(row):
    if ( not pd.isnull(row['Requested'])):
        if ("1" in str(row['Natco'])):
            row['requested_Weekly'] = 1
        elif ("2" in str(row['Natco'])):
            row['requested_Monthly'] = 1
        else:
            row['requested_Daily'] = 1
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
    row['KEY1'] = "{}-{}-{}-{}".format(iso_mapping[row['Natco']],str(row['KPI_ID']).upper(), row['Date'], period)
    return row

def get_lookups(out):
    NATVALUEMAP = out[["Input_ID", "Value"]].drop_duplicates()
    NATTIMEMAP = out[["Input_ID", "Time"]].drop_duplicates()
    NATDELMAP = out[["Input_ID", "Value"]].drop_duplicates()
    NATDELMAP['isDelivered'] = NATDELMAP['Value'].apply(lambda x: 0 if (pd.isnull(x)) else 1)
    NATDELMAP = NATDELMAP[['Input_ID', 'isDelivered']]
    Input_FileMAP = out[["Input_ID", "Input_File"]].drop_duplicates()
    WasCorrectedMAP = out[["Input_ID", "was_corrected_Flag"]].drop_duplicates()
    return [NATVALUEMAP,NATTIMEMAP,NATDELMAP,Input_FileMAP,WasCorrectedMAP]

def all_join(tables):
    result = reduce(lambda left, right: pd.merge(left, right, on=['Input_ID'], how='outer'), tables)
    if ('isDelivered' in result.columns):
        result['isDelivered'].fillna('0')
    if ('Denominator' in result.columns):
        result['Denominator'].fillna('0')
    return result

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

data=pd.read_excel("/Users/ondrejmachacek/tmp/KPI/kpi_request/DTAG-KPI-formular_database-master.xlsx", header=1, sheet_name='PM-data-base')
data_raw = data[[ "EDWH KPI_ID","KPI name","AL","GR","RO","UK","DE","AT","CG","CZ","HR","HU","MK","NL","PL","SK","AL.1",
                  "GR.1","RO.1","UK.1","DE.1","AT.1","CG.1","CZ.1","HR.1","HU.1","MK.1","NL.1","PL.1","SK.1","AL.2","GR.2","RO.2","UK.2",
                  "DE.2","AT.2","CG.2","CZ.2","HR.2","HU.2","MK.2","NL.2","PL.2","SK.2"]]
data_filtered = data_raw[(data_raw["EDWH KPI_ID"] != "") & (data_raw["KPI name"]!= "")]

data_filtered.rename(columns={"EDWH KPI_ID":"KPI_ID", "KPI name":"KPI_Name"}, inplace=True)
cross_tab = data_filtered.melt(id_vars=["KPI_ID", "KPI_Name"], var_name="Natco", value_name="Requested")
cross_tab["requested_Daily"] = 0
cross_tab["requested_Weekly"] = 0
cross_tab["requested_Monthly"] = 0
cross_tab = cross_tab.apply(lambda row: set_requested(row), axis=1)
kpi_database = cross_tab.groupby(['KPI_ID','KPI_Name','Natco']).agg(requested_Daily=("requested_Daily","max"),requested_Weekly=("requested_Weekly","max"), requested_Monthly=("requested_Monthly","max")).reset_index()


sdate = date(2014,1,1)   # start date
edate = date.today()
year=edate.year
calendar_day = pd.DataFrame(data=pd.date_range(sdate,edate-timedelta(days=1),freq='d'), columns=["Date"])
calendar_day['normDate'] = calendar_day['Date'].apply(lambda x: x.strftime('%Y%m%d'))
calendar_day['Week'] = calendar_day['Date'].dt.isocalendar().week
calendar_day['Month'] = calendar_day['Date'].dt.month
calendar_day['Year'] = calendar_day['Date'].dt.year
calendar_day['Day'] = calendar_day['Date'].dt.day
calendar_day['CurYTDFlag'] = calendar_day['Year'].apply(lambda x: check_year(x))
calendar_day['LastYTDFlag'] = calendar_day['Date'].apply(lambda x: check_last_ytd(x))
calendar_day['RC12'] = calendar_day['Year'].apply(lambda x: check_last_year(x))
calendar_day['YearMonth'] = calendar_day['Date'].apply(lambda x: x.strftime('%Y%m'))
calendar_day['Quarter'] = calendar_day['Month'].apply(lambda x: 'Q'+str(math.ceil(x/3)))
calendar_day['YearWeek'] = calendar_day['Date'].apply(lambda x: week_year(x))
calendar_day['WeekDay'] = calendar_day['Date'].dt.dayofweek

calendar_week = calendar_day[calendar_day['WeekDay'] == 0]
calendar_month = calendar_day[calendar_day['Day'] == 1]

matrix_month = kpi_database[['Natco','KPI_ID','KPI_Name', 'requested_Monthly' ]].merge(calendar_month[['Date']], how='cross').reset_index()
matrix_week = kpi_database[['Natco','KPI_ID','KPI_Name', 'requested_Weekly' ]].merge(calendar_week[['Date']], how='cross').reset_index()
matrix_day = kpi_database[['Natco','KPI_ID','KPI_Name', 'requested_Weekly' ]].merge(calendar_day[['Date']], how='cross').reset_index()

daily_out = pd.read_csv("/Users/ondrejmachacek/tmp/KPI/outs/TMA_daily_13-7-2021.csv", delimiter='|', header=0).rename(columns={"Date": "Time"})

DenominatorMAP= daily_out[["Input_ID","Denominator"]].drop_duplicates()
NumeratorMAP= daily_out[["Input_ID","Numerator"]].drop_duplicates()
SourceSystemMAP= daily_out[["Input_ID","SourceSystem"]].drop_duplicates()

matrix_day['KEY1'] = None

matrix_day_enriched = matrix_day[matrix_day['Natco'] == natCo].apply(lambda row: get_key(row, 'd'), axis=1)

to_join = [matrix_day_enriched.rename(columns={'KEY1':'Input_ID'})] +get_lookups(daily_out) +[DenominatorMAP,NumeratorMAP,SourceSystemMAP]

result_daily = all_join(to_join)

weekly_out = pd.read_csv("/Users/ondrejmachacek/tmp/KPI/outs/TMA_daily_13-7-2021.csv", delimiter='|', header=0).rename(columns={"Date": "Time"})
RemarksMAP = weekly_out[["Input_ID","Remarks"]].drop_duplicates()

matrix_week_enriched = matrix_week[matrix_week['Natco'] == natCo].apply(lambda row: get_key(row, 'w'), axis=1)
to_join = [matrix_week_enriched.rename(columns={'KEY1':'Input_ID'})] + get_lookups(weekly_out)+ [RemarksMAP]
result_weekly = all_join(to_join)

monthly_out = pd.read_csv("/Users/ondrejmachacek/tmp/KPI/outs/TMA_daily_13-7-2021.csv", delimiter='|', header=0).rename(columns={"Date": "Time"})
RemarksMAP = weekly_out[["Input_ID","Remarks"]].drop_duplicates()

matrix_month_enriched = matrix_month[matrix_month['Natco'] == natCo].apply(lambda row: get_key(row, 'm'), axis=1)
to_join = [matrix_month_enriched.rename(columns={'KEY1':'Input_ID'})] +get_lookups(monthly_out) +[RemarksMAP]
result_monthy = all_join(to_join)

print(result_daily)

for i in result_daily.columns:
    print(i)