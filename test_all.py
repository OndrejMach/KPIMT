from Weekly_avgs import Weekly_avgs
from KPIs_reader import KPI_reader
from Weekly_input import Weekly_input
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

kpis = KPI_reader("/Users/ondrejmachacek/tmp/KPI/kpi_request/").read_data()

print(kpis.columns)

input_daily = pd.read_csv("/Users/ondrejmachacek/tmp/KPI/output/TMA_daily.csv",delimiter='|', header=0, dtype=str)

print(input_daily.columns)

output_weekly = pd.read_csv("/Users/ondrejmachacek/tmp/KPI/output/COSGRE_weekly.csv",delimiter='|', header=0, dtype=str)

print(output_weekly.columns)
proc = Weekly_avgs(kpis=kpis,dailyOutput=input_daily, weeklyOutput=output_weekly)

#res = proc.process_data()

#res['weekly_out'].to_csv('/Users/ondrejmachacek/tmp/KPI/output/haha.csv',sep='|')
correnctions_path="/Users/ondrejmachacek/tmp/KPI/correction/Corrections26072021.csv"
corrections = pd.read_csv(correnctions_path,delimiter='|', header=0, dtype=str)

input_weekly = pd.read_excel("/Users/ondrejmachacek/tmp/KPI/input/COSGRE/weekly_input/PF_Data_2021-07-12-COSGRE.xlsx", header=0, dtype=str)

print(input_weekly.columns)
print("COUNT: "+ str(input_weekly.shape[0]))
res = Weekly_input(weekly_output=output_weekly, filename="PF_Data_2021-07-12-COSGRE.xlsx",
                   filename_timestamp="blabla", raw_input=input_weekly, natco="COSGRE", corrections=corrections
                   ).process_data()


