import math
import pandas as pd
from datetime import date, timedelta
from sit.kpimt.matrixFunctions import check_year, check_last_year, check_last_ytd, set_requested, week_year, get_key, \
    get_lookups, all_join, iso_mapping
from sit.kpimt.confs import matrix_schema_daily,matrix_schema_weekly, matrix_schema_monthly


class MatrixGeneratorDaily:
    kpis = None
    daily_output = None
    monthly_output = None
    weekly_output = None
    natCo = None

    def __init__(self, kpis, daily_output, weekly_output, monthly_output, natCo):
        self.kpis = kpis
        self.daily_output = daily_output
        self.weekly_output = weekly_output
        self.monthly_output = monthly_output
        self.natCo = natCo

    def processing(self):
        data_raw = self.kpis[
            ["EDWH KPI_ID", "KPI name", "AL", "GR", "RO", "UK", "DE", "AT", "CG", "CZ", "HR", "HU", "MK", "NL", "PL",
             "SK", "AL.1",
             "GR.1", "RO.1", "UK.1", "DE.1", "AT.1", "CG.1", "CZ.1", "HR.1", "HU.1", "MK.1", "NL.1", "PL.1", "SK.1",
             "AL.2", "GR.2", "RO.2", "UK.2",
             "DE.2", "AT.2", "CG.2", "CZ.2", "HR.2", "HU.2", "MK.2", "NL.2", "PL.2", "SK.2"]]
        data_filtered = data_raw[(data_raw["EDWH KPI_ID"] != "") & (data_raw["KPI name"] != "")]

        data_filtered.rename(columns={"EDWH KPI_ID": "KPI_ID", "KPI name": "KPI_Name"}, inplace=True)
        cross_tab = data_filtered.melt(id_vars=["KPI_ID", "KPI_Name"], var_name="Natco", value_name="Requested")
        cross_tab["requested_Daily"] = 0
        cross_tab["requested_Weekly"] = 0
        cross_tab["requested_Monthly"] = 0
        cross_tab = cross_tab.apply(lambda row: set_requested(row), axis=1)
        kpi_database = cross_tab.groupby(['KPI_ID', 'KPI_Name', 'Natco']).agg(
            requested_Daily=("requested_Daily", "max"), requested_Weekly=("requested_Weekly", "max"),
            requested_Monthly=("requested_Monthly", "max")).reset_index()
        kpi_database['Natco'] = kpi_database['Natco'].apply(lambda x: iso_mapping[x])
        kpi_database = kpi_database[kpi_database['Natco'] == self.natCo].copy()
        print("CROSS_TAB INFO:")
        print(kpi_database.info())
        print(kpi_database.info)

        sdate = date(2014, 1, 1)  # start date
        edate = date.today()
        year = edate.year
        calendar_day = pd.DataFrame(data=pd.date_range(sdate, edate - timedelta(days=1), freq='d'), columns=["Date"])
        calendar_day['normDate'] = calendar_day['Date'].apply(lambda x: x.strftime('%Y%m%d'))
        calendar_day['Week'] = calendar_day['Date'].dt.isocalendar().week
        calendar_day['Month'] = calendar_day['Date'].dt.month
        calendar_day['Year'] = calendar_day['Date'].dt.year
        calendar_day['Day'] = calendar_day['Date'].dt.day
        calendar_day['CurYTDFlag'] = calendar_day['Year'].apply(lambda x: check_year(x))
        calendar_day['LastYTDFlag'] = calendar_day['Date'].apply(lambda x: check_last_ytd(x))
        calendar_day['RC12'] = calendar_day['Year'].apply(lambda x: check_last_year(x))
        calendar_day['YearMonth'] = calendar_day['Date'].apply(lambda x: x.strftime('%Y%m'))
        calendar_day['Quarter'] = calendar_day['Month'].apply(lambda x: 'Q' + str(math.ceil(x / 3)))
        calendar_day['YearWeek'] = calendar_day['Date'].apply(lambda x: week_year(x))
        calendar_day['WeekDay'] = calendar_day['Date'].dt.dayofweek
        print("CALEANDAR INFO:")
        print(calendar_day.info())
        print(calendar_day.info)


        calendar_week = calendar_day[calendar_day['WeekDay'] == 0].copy()
        calendar_month = calendar_day[calendar_day['Day'] == 1].copy()

        kpi_database['tmp'] = 1
        calendar_week['tmp'] = 1
        calendar_month['tmp'] = 1
        calendar_day['tmp'] = 1

        matrix_month = pd.merge(kpi_database[['Natco', 'KPI_ID', 'KPI_Name', 'requested_Monthly', 'tmp']],
                                calendar_month[['Date', 'tmp']], on=['tmp'])
        matrix_week = pd.merge(kpi_database[['Natco', 'KPI_ID', 'KPI_Name', 'requested_Weekly', 'tmp']],
            calendar_week[['Date','tmp']],on=['tmp'])
        matrix_day = pd.merge(kpi_database[['Natco', 'KPI_ID', 'KPI_Name', 'requested_Daily', 'tmp']],
            calendar_day, on=['tmp'])
        matrix_month.drop(columns=['tmp'], inplace=True)
        matrix_week.drop(columns=['tmp'], inplace=True)
        matrix_day.drop(columns=['tmp'], inplace=True)
        print("JOIN WITH CALENDAR DONE")
        print(matrix_day.info())
        print(matrix_day.info)

        # daily_out = pd.read_csv("/Users/ondrejmachacek/tmp/KPI/outs/TMA_daily_13-7-2021.csv", delimiter='|',
        #      header=0).rename(columns={"Date": "Time"})
        result_daily = None
        result_weekly = None
        result_monthly = None
        if (self.daily_output is not None):
            print("CALCULATING DAILY MATRIX")
            daily_out = self.daily_output.rename(columns={"Date": "Time"})
            print("DAILY_OUTPUT :")
            print(daily_out.info())
            print(daily_out.info)
            DenominatorMAP = daily_out[["Input_ID", "Denominator"]].drop_duplicates()
            NumeratorMAP = daily_out[["Input_ID", "Numerator"]].drop_duplicates()
            SourceSystemMAP = daily_out[["Input_ID", "SourceSystem"]].drop_duplicates()
            print("DenominatorMAP :")
            print(DenominatorMAP.info())
            print(DenominatorMAP.info)
            print("NumeratorMAP :")
            print(NumeratorMAP.info())
            print(NumeratorMAP.info)
            print("SourceSystemMAP :")
            print(SourceSystemMAP.info())
            print(SourceSystemMAP.info)

            matrix_day['KEY1'] = None
            matrix_day_enriched = matrix_day.apply(lambda row: get_key(row, 'd'),axis=1)
            #matrix_day_enriched = matrix_day_enriched[matrix_day_enriched['Natco'] == self.natCo]
            matrix_day_enriched.rename(columns={'KEY1': 'Input_ID', }, inplace=True)
            print("BEFORE JOINS: ")
            print(matrix_day_enriched.info())
            print(matrix_day_enriched.info)
            to_join =  get_lookups(daily_out) + [
                DenominatorMAP,
                NumeratorMAP,
                SourceSystemMAP]
            result_daily = all_join(matrix_day_enriched,to_join)
            print("AFTER JOINS:  ")
            print(result_daily.info())
            print(result_daily.info)
            result_daily.rename(columns={'Input_ID': 'KEY1', 'Value': 'KPI_Value', "isDelivered":"IsDelivered"}, inplace=True)

            result_daily = result_daily[matrix_schema_daily]
        # weekly_out = pd.read_csv("/Users/ondrejmachacek/tmp/KPI/outs/TMA_daily_13-7-2021.csv", delimiter='|',
        #                        header=0).rename(columns={"Date": "Time"})
        if (self.weekly_output is not None):
            print("CALCULATING WEEKLY MATRIX")
            weekly_out = self.weekly_output.rename(columns={"Date": "Time"})
            RemarksMAP = weekly_out[["Input_ID", "Remarks"]].drop_duplicates()

            matrix_week['KEY1'] = None
            matrix_week_enriched = matrix_week.apply(lambda row: get_key(row, 'w'),
                                                                                         axis=1)
            #matrix_week_enriched = matrix_week_enriched[matrix_week_enriched['Natco'] == self.natCo]
            matrix_week_enriched.rename(columns={'KEY1': 'Input_ID'}, inplace=True)
            to_join =  get_lookups(weekly_out) + [RemarksMAP]
            result_weekly = all_join(matrix_week_enriched,to_join)
            result_weekly.rename(columns={'Input_ID': 'KEY1','Value': 'KPI_Value', "isDelivered":"IsDelivered"}, inplace=True)
            result_weekly = result_weekly[matrix_schema_weekly]

        if (self.monthly_output is not None):
            print("CALCULATING MONTHLY MATRIX")
            monthly_out = self.monthly_output.rename(columns={
                "Date": "Time"})  # pd.read_csv("/Users/ondrejmachacek/tmp/KPI/outs/TMA_daily_13-7-2021.csv", delimiter='|',
            #           header=0).rename(columns={"Date": "Time"})
            RemarksMAP = weekly_out[["Input_ID", "Remarks"]].drop_duplicates()

            matrix_month['KEY1'] = None

            matrix_month_enriched = matrix_month.apply(lambda row: get_key(row, 'm'), axis=1)
            #matrix_month_enriched = matrix_month_enriched[matrix_month_enriched['Natco'] == self.natCo]
            matrix_month_enriched.rename(columns={'KEY1': 'Input_ID'}, inplace=True)
            to_join =  get_lookups(monthly_out) + [
                RemarksMAP]
            result_monthly = all_join(matrix_month_enriched,to_join)
            result_monthly.rename(columns={'Input_ID': 'KEY1','Value': 'KPI_Value', "isDelivered": "IsDelivered"}, inplace=True)
            result_monthly = result_monthly[matrix_schema_monthly]


        return {"daily_matrix": result_daily, "weekly_matrix": result_weekly, "monthly_matrix": result_monthly}
