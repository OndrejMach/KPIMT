import pandas as pd

class Facts:
    kpis= None
    all_weekly_data = None
    ims = None

    def __init__(self, all_weekly_data, ims_data,kpis):
        self.kpis=kpis
        self.all_weekly_data=all_weekly_data
        self.ims=ims_data

    def process_data(self):
        def getId(row):
            row['%SVKPI_Key'] = str(row['Service']) + '-' + str(row['Vendor']) + '-' + str(row['KPI name'])
            return row

        mapping_polarity = {'F1' : "F2", 'yes' : "more is better", 'no' : "less is better"}
        # Create DataFrame.


        kpi_input = self.kpis[['KPI name', 'PM Report', 'Vendor', 'Service', 'Unit', '[13] More is better' ]].copy()
        kpi_input['KPI name'] = kpi_input['KPI name'].apply(lambda x: str(x).upper())
        kpi_input.rename(columns={'Unit': 'Units', '[13] More is better' : "Polarity"}, inplace=True)
        kpi_input['%SVKPI_Key'] = None
        kpi_input = kpi_input.apply(lambda x: getId(x), axis=1)
        print(kpi_input['Polarity'])
        kpi_input['Polarity'] = kpi_input['Polarity'].apply(lambda x: mapping_polarity[x] if pd.notna(x) else '')
        kpi_input_filtered = kpi_input[pd.notna(kpi_input['KPI name']) & (kpi_input['KPI name'] != '') & (pd.notna(kpi_input['Vendor'])) & (kpi_input['Vendor'] != '') & (pd.notna(kpi_input['Service'])) & (kpi_input['Service'] != '')]

        weekly_out = self.all_weekly_data[['Date', 'KPI name', 'Value', 'Region', 'Input_ID']].copy()
        ims_out = self.ims[['Date', 'KPI name', 'Value', 'Region', 'Input_ID']].copy()
        ims_out['Input_ID'] = ims_out['Input_ID'].apply(lambda x: str(x) + '-w')
        weekly_all = pd.concat([weekly_out,ims_out ], axis=0, ignore_index=True)

        joined = pd.merge(weekly_all, kpi_input_filtered, on='KPI name')
        joined['dummi'] = '1'
        joined = joined[((joined['PM Report'] == 'Y') | (joined['PM Report'] == 'y')) & ((pd.notna(joined['%SVKPI_Key']))) & (joined['%SVKPI_Key'] != '--')]
        result = joined[['Date', 'KPI name', 'Value', 'Region', 'Input_ID', 'Vendor', 'Service', 'Units', 'Polarity', '%SVKPI_Key','PM Report']]
        result.rename(columns={'KPI name' : 'Kpi name'}, inplace=True)


        print(result.columns)
        print(result.info)
        return result
