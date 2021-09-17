import csv
import os

import pandas as pd
from glob import  glob
from sit.kpimt.KPIs_reader import KPI_reader
from sit.kpimt.Output_processor import Output_processor
from sit.kpimt.confs import outputs, natcos
from os import path
from sit.kpimt.MatrixGenerator import MatrixGeneratorDaily
from sit.kpimt.Weekly_avgs import Weekly_avgs
from sit.kpimt.Monthly_avgs import Monthly_avgs
from sit.kpimt.Multimarket import Multimarket
from sit.kpimt.functions import get_file_timestamp
from sit.kpimt.IMS_processing import IMS_processing
from sit.kpimt.KPI_Report import KPI_Report
from sit.kpimt.Facts import Facts
from datetime import  datetime




params = {
    "kpis_path": "/Users/ondrejmachacek/tmp/KPI/kpi_request/",
    "correnctions_path": "/Users/ondrejmachacek/tmp/KPI/correction/",
    'basepath': "/Users/ondrejmachacek/tmp/KPI/input",
    "output_path": "/Users/ondrejmachacek/tmp/KPI/out_ref/",
    'multimarket': "/Users/ondrejmachacek/tmp/KPI/multimarket/",
    'multimarket_archive': "/Users/ondrejmachacek/tmp/KPI/multimarket/",
    'ims_path' : "/Users/ondrejmachacek/tmp/KPI/IMS",
    'ims_archive': "/Users/ondrejmachacek/tmp/KPI/IMS/",
    'reports_path' :'/Users/ondrejmachacek/tmp/KPI/output/reports'
}

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

def run_outputs_processing(natco, mode, params):
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    print(pd.__version__)
    # print(format_date("01/02/1984"))
    proc = Output_processor()
    # print(args.natco)
    print("running: " +mode)
    if (mode == 'daily_input'):
        return proc.run_input_processing(natco=natco, mode=mode, basepath=params["basepath"], output_path=params['output_path'], corrections_path=params["correnctions_path"],
                             period="daily")
    elif (mode == 'weekly_input'):
        return proc.run_input_processing(natco=natco, mode=mode, basepath=params["basepath"],  output_path=params['output_path'], corrections_path=params["correnctions_path"],
                             period="weekly")
    elif (mode == 'monthly_input'):
        return proc.run_input_processing(natco=natco, mode=mode, basepath=params["basepath"],  output_path=params['output_path'], corrections_path=params["correnctions_path"],
                             period="monthly")


def run_matrix_processing(natco, params):
    def get_output_data(file):
        if (path.exists(file)):
            return pd.read_csv(file, delimiter='|', header=0, dtype=str)
        else:
            return None
    def get_output_path(type):
        return '{}/{}{}'.format(params['output_path'], natco, outputs[type])

    def get_matrix_file(period):
        return "{}/Matrix/{}_Matrix_{}.csv".format(params['output_path'], natco,period)

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    print(pd.__version__)
    kpis = pd.read_excel(params['kpis_path']+"/DTAG-KPI-formular_database-master.xlsx", header=1, sheet_name='PM-data-base')
    print(kpis.columns)
    daily_output_file = get_output_path('daily_input')
    weekly_output_file = get_output_path('weekly_input')
    monthly_output_file = get_output_path('monthly_input')
    daily_output = get_output_data(daily_output_file)
    weekly_output = get_output_data(weekly_output_file)
    monthly_output = get_output_data(monthly_output_file)
    proc = MatrixGeneratorDaily(kpis=kpis,daily_output=daily_output, weekly_output=weekly_output, monthly_output=monthly_output, natCo=natco)
    data = proc.processing()

    if (data['daily_matrix'] is not None):
        matrix_file = get_matrix_file("daily")
        data['daily_matrix'].to_csv(matrix_file, sep="|", index=False)
    if (data['weekly_matrix'] is not None):
        matrix_file = get_matrix_file("weekly")
        data['weekly_matrix'].to_csv(matrix_file, sep="|", index=False)
    if (data['monthly_matrix'] is not None):
        matrix_file = get_matrix_file("monthly")
        data['monthly_matrix'].to_csv(matrix_file, sep="|", index=False)

def run_avg_processing(params, natco, mode):
    daily_out_file = "{}/{}_daily.csv".format(params['output_path'], natco)
    out_file = "{}/{}_{}.csv".format(params['output_path'], natco, mode)
    if (path.exists(daily_out_file) and path.exists(out_file)):
        daily = pd.read_csv(daily_out_file, delimiter='|', header=0, dtype=str)
        out_data = pd.read_csv(out_file, delimiter='|', header=0, dtype=str)
        kpis = KPI_reader(params['kpis_path']).read_data()

        avg_proc = Weekly_avgs(dailyOutput=daily, weeklyOutput=out_data, kpis=kpis ) if (mode == 'weekly') else Monthly_avgs(dailyOutput=daily, monthlyOutput=out_data, kpis=kpis )
        result = avg_proc.process_data()
        avg_file = "{}/{}_{}_averages_from_daily_input.csv".format(params['output_path'],natco, mode)
        result['averages'].to_csv(avg_file, sep="|", index=False)
        result['out'].to_csv(out_file, sep="|", index=False)

def run_multimarket(params):
    print("STARTING MULTIMARKET PROCESSING")
    multimarket_natcos = natcos #["TMA","TMCZ","TMHR","COSROM"]
    all_files = glob(params['output_path'] + "/*monthly.csv")
    li = []
    for filename in all_files:
        print("READING INPUT "+filename)
        df = pd.read_csv(filename,delimiter='|', header=0, dtype=str)
        li.append(df)
    all_monthly = pd.concat(li, axis=0, ignore_index=True)
    multimarket_files= glob(params['multimarket'] + "/*.xlsx")
    corrections = pd.read_csv(params['correnctions_path']+"/Corrections.csv", delimiter='|', header=0, dtype=str)
    for filename in multimarket_files:
        print("PROCESSING FILENAME "+filename)
        multimarket_input_data = pd.read_excel(filename, header=0)
        data = Multimarket(multimarket_in=multimarket_input_data, corrections=corrections, all_monthly=all_monthly,filename=path.basename(filename), filetime= get_file_timestamp(filename)).process_data()
        print("WRITING CORRECTIONS FILE TO: "+params['correnctions_path']+"/Corrections.csv")
        data['corrections'].to_csv(params['correnctions_path']+"/Corrections.csv", sep="|", index=False)
        for natco in multimarket_natcos:
            print("WRITING OUTPUT FILE FOR "+natco+"  TO: " + params['output_path']+"/"+natco+"_monthly.csv")
            data[natco].to_csv(params['output_path']+"/"+natco+"_monthly.csv", sep="|", index=False)
        os.rename(filename, params['multimarket_archive']+"/"+path.basename(filename)+"_"+datetime.now().strftime('%Y%m%d%H%M%S'))

def run_ims(params):
    print("STARTING IMS PROCESSING ")
    all_files = glob(params['ims_path'] + "/*.xlsx")
    for filename in all_files:
        print("PROCESSING FILENAME " + filename)
        ims_input_data = pd.read_excel(filename, header=0)
        data = IMS_processing(ims_data=ims_input_data).process_data()
        data.to_csv(params['output_path']+"/IMS_facts.csv", sep="|", index=False)
        os.rename(filename, params['ims_archive']+"/"+path.basename(filename)+"_"+datetime.now().strftime('%Y%m%d%H%M%S'))


def generate_report(kpis_path, matrix_path, reports_path):
    kpis = pd.read_excel(kpis_path + "/DTAG-KPI-formular_database-master.xlsx", header=1,
                         sheet_name='PM-data-base')
    natcos_for_report = [f for f in natcos if f not in ['TMD', 'AMC']]
    for natco in natcos_for_report:
        filename = '{}/{}_Matrix_monthly.csv'.format(matrix_path, natco)

        if path.exists(filename):
            print("PROCESSING FILE: "+filename)
            monthly_matrix = pd.read_csv(filename, delimiter='|', header=0, dtype=str)

            data = KPI_Report(kpis=kpis, matrix_monthly=monthly_matrix, natco= natco).process_data()
            output_filename= reports_path + "/KPILoadAndStatusMonitor_"+natco+".xlsx"
            writer = pd.ExcelWriter(output_filename)
            data.to_excel(writer, sheet_name='sheet1', index=False, na_rep='NaN')

            # Auto-adjust columns' width
            for column in data:
                column_width = max(data[column].astype(str).map(len).max(), len(column))
                col_idx = data.columns.get_loc(column)
                writer.sheets['sheet1'].set_column(col_idx, col_idx, column_width)
            writer.save()


            #data.to_excel(output_filename,  index=False)
        else:
            print("FILE: "+filename +" does not exist!!")


def run_facts_processing(kpis_path, output_path):
    kpis = pd.read_excel(kpis_path + "/DTAG-KPI-formular_database-master.xlsx", header=1,
                         sheet_name='PM-data-base')
    all_files = glob(output_path + "*_weekly.csv")
    li = []
    for filename in all_files:
        print("Reading: "+filename)
        df = pd.read_csv(filename, index_col=None, header=0, delimiter="|")
        li.append(df)

    weekly_data = pd.concat(li, axis=0, ignore_index=True)
    ims_path = output_path + "/IMS_facts.csv"
    ims = pd.read_csv(ims_path, index_col=None, header=0, delimiter="|")
    facts = Facts(all_weekly_data=weekly_data, kpis=kpis, ims_data=ims)

    result = facts.process_data()
    result.to_csv(output_path + "/facts.csv", sep="|", index=False, quoting=csv.QUOTE_ALL)





# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    #run_outputs_processing("COSGRE", 'weekly_input', params)
    #kpis = KPI_reader(params['kpis_path']).read_data()
    #run_matrix_processing("TMA", params)
    #run_multimarket(params)
    #print(float(0,5))
    #run_ims(params)
    #generate_report(kpis_path = params['kpis_path'], matrix_path=params['output_path']+"/Matrix/", reports_path=params['reports_path'])
    run_facts_processing(kpis_path=params['kpis_path'], output_path=params['output_path'])

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
