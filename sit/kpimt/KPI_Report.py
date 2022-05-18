from datetime import date, timedelta, datetime
import dateutil.relativedelta
import pandas as pd

natcoMapping = {
    "TMCG": "CG",
    "COSROM": "RO",
    "TMA": "AT",
    "COSGRE": "GR",
    "TMCZ": "CZ",
    "TMHR": "HR",
    "TMHU": "HU",
    "TMMK": "MK",
    "TMNL": "NL",
    "TMPL": "PL",
    "TMSK": "SK"
}


class KPI_Report:
    kpis = None
    matrix_monthly = None
    mappings = None
    natco = None

    def __init__(self, kpis, matrix_monthly, natco, mappings):
        self.kpis = kpis
        self.matrix_monthly = matrix_monthly
        self.natco = natco
        self.mappings = mappings

    def process_data(self):
        def get_minus_months(months, format='%Y-%m-01'):
            last_month = date.today() - dateutil.relativedelta.relativedelta(months=months)
            return last_month.strftime(format)

        def evaluate_threshold(value, threshold, more):
            if (pd.isna(threshold) or threshold == ''):
                return True
            elif (pd.isna(more) or more == '' or (more != 'yes' and more != 'no')):
                return True
            elif (more == 'yes' and float(threshold) <= float(value)):
                return True
            elif (more == 'no' and float(threshold) >= float(value)):
                return True
            else:
                return False

        def get_status(row):
            if (row['Requested'] == 'true' and row['Delivered'] == 'false'):
                row['Last Month Status'] = 'MISSING'
            elif (row['Requested'] == 'false' and row['Delivered'] == 'true'):
                row['Last Month Status'] = 'Not requested'
            elif (row['Requested'] == 'false' and row['Delivered'] == 'false'):
                row['Last Month Status'] = "Not requested"
            elif (row['Requested'] == 'true' and row['Delivered'] == 'true' and evaluate_threshold(row[last_month_col],
                                                                                                   row['Threshold'],
                                                                                                   row['more'])):
                row['Last Month Status'] = 'OK'
            else:
                row['Last Month Status'] = "SLA Breach"
            return row

        print("MATRIX INFO:")
        print(self.matrix_monthly.info())
        print(self.matrix_monthly.info)
        country = natcoMapping[str.upper(self.natco)]
        kpis = self.kpis[['Unit', 'EDWH KPI_ID', 'iSLA2.0', '[13] More is better']].copy()
        kpis.rename(columns={'EDWH KPI_ID': 'KPI_ID', '[13] More is better': 'more'}, inplace=True)
        thresholds = self.mappings[["EDWH KPI_ID", "Threshold", 'Threshold flag', country]].copy()
        #thresholds.rename(columns=lambda c: thresholds[c].pop(0) if c in thresholds.keys() else c, inplace=True)
        print(thresholds.info())
        print(thresholds.info)
        ountry = country + "1"
        thresholds = thresholds[pd.notna(thresholds[country])].copy()
        thresholds = thresholds[thresholds['Threshold flag'] == 'T'].copy()
        thresholds.rename(columns={'EDWH KPI_ID': 'KPI_ID'}, inplace=True)

        print("KPIS INFO:")
        print(kpis.info())
        print(kpis.info)

        print("MAPPINGS INFO:")
        print(thresholds.info())
        print(thresholds.info)

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

        last_month_matrix['Requested'] = last_month_matrix['requested_Monthly'].apply(
            lambda x: "true" if x == '1' else "false")
        last_month_matrix['Delivered'] = last_month_matrix['IsDelivered'].apply(
            lambda x: "true" if x == '1' else "false")
        last_month_matrix[last_month_col] = last_month_matrix['KPI_Value']

        print("LAST MONTH MATRIX:")
        print(last_month_matrix.info())

        last_2_month_matrix = self.matrix_monthly[self.matrix_monthly['Date'] == last_2_month].copy()
        last_2_month_matrix = last_2_month_matrix[['KPI_ID', 'KPI_Value']].drop_duplicates()
        last_2_month_matrix.rename(columns={"KPI_Value": last_2_month_col}, inplace=True)
        print("LAST 2 MONTH MATRIX:")
        print(last_2_month_matrix.info())

        last_3_month_matrix = self.matrix_monthly[
            self.matrix_monthly['Date'] == last_3_month].copy()
        last_3_month_matrix = last_3_month_matrix[['KPI_ID', 'KPI_Value']].drop_duplicates()
        last_3_month_matrix.rename(columns={"KPI_Value": last_3_month_col}, inplace=True)
        print("LAST 3 MONTH MATRIX:")
        print(last_3_month_matrix.info())

        requested = last_month_matrix[last_month_matrix['Requested'] == 'true']

        enriched = pd.merge(requested, last_2_month_matrix, on=['KPI_ID'], how="left")
        enriched = pd.merge(enriched, last_3_month_matrix, on=['KPI_ID'], how="left")
        enriched = pd.merge(enriched, kpis, on=['KPI_ID'], how="left")
        enriched = pd.merge(enriched, thresholds, on=['KPI_ID'], how='left')
        enriched['Last Month Status'] = None
        print('ENRICHED INFO:')
        print(enriched.info())

        result = enriched.apply(lambda row: get_status(row), axis=1)
        # result['KPI_Name'] = result['KPI_Name'].apply(lambda x: str(x).upper())
        result = result[
            ['Last Month Status', 'KPI_Name', last_month_col, last_2_month_col, last_3_month_col, 'Unit', 'Threshold']]
        print('RESULT INFO:')
        print(result.info())
        print(result.info)

        return result
