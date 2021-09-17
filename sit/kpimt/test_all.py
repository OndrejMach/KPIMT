from sit.kpimt.Weekly_avgs import Weekly_avgs
from sit.kpimt.KPIs_reader import KPI_reader
from sit.kpimt.Weekly_input import Weekly_input
from sit.kpimt.MatrixGenerator import MatrixGeneratorDaily
import pandas as pd
import glob
from Facts import Facts

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

#kpis = KPI_reader("/Users/ondrejmachacek/tmp/KPI/kpi_request/").read_data()
kpis = pd.read_excel("/Users/ondrejmachacek/tmp/KPI/kpi_request/DTAG-KPI-formular_database-master.xlsx", header=1, sheet_name='PM-data-base')
print(kpis.columns)

path = r'/Users/ondrejmachacek/tmp/KPI/out_ref/' # use your path
all_files = glob.glob(path + "*_weekly.csv")

li = []

for filename in all_files:
    print(filename)
    df = pd.read_csv(filename, index_col=None, header=0, delimiter="|")
    li.append(df)

weekly_data = pd.concat(li, axis=0, ignore_index=True)
ims_path = path +"IMS_facts.csv"
ims = pd.read_csv(ims_path, index_col=None, header=0, delimiter="|")


#print(weekly_out.info)

facts = Facts(all_weekly_data=weekly_data, kpis=kpis, ims_data=ims)

facts.process_data()

# input_daily = pd.read_csv("/Users/ondrejmachacek/tmp/KPI/output/TMPL_daily.csv",delimiter='|', header=0, dtype=str)
#
# print(input_daily.columns)
#
# output_weekly = pd.read_csv("/Users/ondrejmachacek/tmp/KPI/output/TMPL_weekly.csv",delimiter='|', header=0, dtype=str)
#
# print(output_weekly.columns)
#
# matrix = MatrixGeneratorDaily(kpis=kpis, daily_output=input_daily, weekly_output=output_weekly, monthly_output=None, natCo="TMPL").processing()
# print(matrix['daily_matrix'].info())
#
# matrix['daily_matrix'].to_csv("/Users/ondrejmachacek/tmp/KPI/output/TMPL_test.csv", sep="|", header=True)





#proc = Weekly_avgs(kpis=kpis,dailyOutput=input_daily, weeklyOutput=output_weekly)

#res = proc.process_data()

#res['weekly_out'].to_csv('/Users/ondrejmachacek/tmp/KPI/output/haha.csv',sep='|')
#correnctions_path="/Users/ondrejmachacek/tmp/KPI/correction/Corrections26072021.csv"
#corrections = pd.read_csv(correnctions_path,delimiter='|', header=0, dtype=str)

#input_weekly = pd.read_excel("/Users/ondrejmachacek/tmp/KPI/input/COSGRE/weekly_input/PF_Data_2021-07-12-COSGRE.xlsx", header=0, dtype=str)

#print(input_weekly.columns)
#print("COUNT: "+ str(input_weekly.shape[0]))
#res = Weekly_input(weekly_output=output_weekly, filename="PF_Data_2021-07-12-COSGRE.xlsx",
#                   filename_timestamp="blabla", raw_input=input_weekly, natco="COSGRE", corrections=corrections
#                   ).process_data()


