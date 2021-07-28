import pandas as pd

class KPI_reader:
    path=None
    def __init__(self, path):
        self.path=path

    def read_data(self):
        print("Reading data from "+self.path + 'DTAG-KPI-formular_database-master.xlsx')
        raw = pd.read_excel(self.path+'DTAG-KPI-formular_database-master.xlsx',
                      sheet_name='PM-data-base', skiprows=0, header=1, usecols='H:I'
                      ).rename(columns={'EDWH KPI_ID': 'EDWH_KPI_ID', 'KPI name': 'EDWH_KPI_Name'})

        kpis_filtered = raw[raw.EDWH_KPI_ID.notnull()]
        kpis_filtered= kpis_filtered.applymap(lambda x: x.strip().upper() if isinstance(x, str) else x)
        return kpis_filtered
