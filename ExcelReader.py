import pandas as pd

input = pd.read_excel('/Users/ondrejmachacek/tmp/KPI/input/COSGRE/monthly_input/PF_Data_2017-12_GR.XLS')
kpis = pd.read_excel('/Users/ondrejmachacek/tmp/KPI/kpi_request/DTAG-KPI-formular_database-master.xlsx',
                     sheet_name='PM-data-base',skiprows = 0, header=1, usecols='H:I'
                     ).rename(columns={'EDWH KPI_ID': 'EDWH_KPI_ID', 'KPI name': 'EDWH_KPI_Name'})

daily_output pd.read_csv()

kpis_filtered = kpis[kpis.EDWH_KPI_ID.notnull()]
kpis_trimmed = kpis_filtered.applymap(lambda x: x.strip() if isinstance(x, str) else x)
print(kpis_trimmed)
#print(kpis)


