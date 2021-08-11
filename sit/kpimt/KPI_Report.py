from datetime import date, timedelta, datetime
import dateutil.relativedelta
import pandas as pd


class KPI_Report:
    kpis = None
    matrix_monthly=  None
    natco = None

    def __init__(self, kpis, matrix_monthly, natco):
        self.kpis = kpis
        self.matrix_monthly = matrix_monthly
        self.natco = natco

    def process_data(self):
        def get_minus_months(months, format = '%Y-%m-01'):
            last_month = date.today() - dateutil.relativedelta.relativedelta(months=months)
            return last_month.strftime(format)

        def get_status(row):
            if (row['Requested'] == 'true' and row['Delivered'] == 'false'):
                row['Last Month Status'] = 'MISSING'
            elif (row['Requested'] == 'false' and row['Delivered'] == 'true'):
                row['Last Month Status'] = 'Not requested'
            elif (row['Requested'] == 'true' and row['Delivered'] == 'true'):
                row['Last Month Status'] = 'OK'
            else:
                row['Last Month Status'] = "Not requested"
            return row


        print("MATRIX INFO:")
        print(self.matrix_monthly.info())
        print(self.matrix_monthly.info)

        kpis = self.kpis[['Unit', 'EDWH KPI_ID', 'iSLA2.0']].copy()
        kpis.rename(columns={'EDWH KPI_ID' : 'KPI_ID'}, inplace=True)

        print("KPIS INFO:")
        print(kpis.info())
        print(kpis.info)

        last_month = get_minus_months(1)
        last_2_month = get_minus_months(2)
        last_3_month = get_minus_months(3)

        last_month_col = get_minus_months(1, '%Y-%b')
        last_2_month_col = get_minus_months(2, '%Y-%b')
        last_3_month_col = get_minus_months(3, '%Y-%b')

        print(last_month)
        print(last_2_month)
        print(last_3_month)

        last_month_matrix = self.matrix_monthly[self.matrix_monthly['Date'] == last_month].copy()

        last_month_matrix['Requested'] = last_month_matrix['requested_Monthly'].apply(lambda x: "true" if x=='1' else "false")
        last_month_matrix['Delivered'] = last_month_matrix['IsDelivered'].apply(
            lambda x: "true" if x == '1' else "false")
        last_month_matrix[last_month_col] = last_month_matrix['KPI_Value']

        print("LAST MONTH MATRIX:")
        print(last_month_matrix.info())

        last_2_month_matrix =  self.matrix_monthly[self.matrix_monthly['Date'] == last_2_month].copy()
        last_2_month_matrix = last_2_month_matrix[['KPI_ID', 'KPI_Value']].drop_duplicates()
        last_2_month_matrix.rename(columns={"KPI_Value" : last_2_month_col}, inplace=True)
        print("LAST 2 MONTH MATRIX:")
        print(last_2_month_matrix.info())

        last_3_month_matrix = self.matrix_monthly[
            self.matrix_monthly['Date'] == last_3_month].copy()
        last_3_month_matrix = last_3_month_matrix[['KPI_ID', 'KPI_Value']].drop_duplicates()
        last_3_month_matrix.rename(columns={"KPI_Value": last_3_month_col}, inplace=True)
        print("LAST 3 MONTH MATRIX:")
        print(last_3_month_matrix.info())

        requested = last_month_matrix[last_month_matrix['Requested'] == 'true']

        enriched = pd.merge(requested,last_2_month_matrix ,on=['KPI_ID'], how="left")
        enriched = pd.merge(enriched,last_3_month_matrix ,on=['KPI_ID'], how="left")
        enriched = pd.merge(enriched,kpis ,on=['KPI_ID'], how="left")
        enriched['Last Month Status'] = None
        print('ENRICHED INFO:')
        print(enriched.info())

        result = enriched.apply(lambda row: get_status(row), axis=1)
        #result['KPI_Name'] = result['KPI_Name'].apply(lambda x: str(x).upper())
        result = result[['Last Month Status', 'KPI_Name',last_month_col,last_2_month_col, last_3_month_col,'Unit']]
        print('RESULT INFO:')
        print(result.info())
        print(result.info)

        return result




