import pandas as pd

from KPIs_reader import KPI_reader
from Output_processor import Output_processor
from confs import outputs
from os import path
from MatrixGenerator import MatrixGeneratorDaily
from Weekly_avgs import Weekly_avgs



params = {
    "kpis_path": "/Users/ondrejmachacek/tmp/KPI/kpi_request/",
    "correnctions_path": "/Users/ondrejmachacek/tmp/KPI/correction/",
    'basepath': "/Users/ondrejmachacek/tmp/KPI/input",
    "output_path": "/Users/ondrejmachacek/tmp/KPI/output/"
}


def run_outputs_processing(natco, mode, params):
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    print(pd.__version__)
    # print(format_date("01/02/1984"))
    proc = Output_processor()
    # print(args.natco)
    if (mode == 'daily_input'):
        proc.run_input_processing(natco=natco, mode=mode, basepath=params["basepath"], output_path=params['output_path'], corrections_path=params["correnctions_path"],
                             period="daily")
    elif (mode == 'weekly_input'):
        proc.run_input_processing(natco=natco, mode=mode, basepath=params["basepath"],  output_path=params['output_path'], corrections_path=params["correnctions_path"],
                             period="weekly")
    elif (mode == 'monthly_input'):
        proc.run_input_processing(natco=natco, mode=mode, basepath=params["basepath"],  output_path=params['output_path'], corrections_path=params["correnctions_path"],
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

def run_avg_processing(params, natco):
    daily_out_file = "{}/{}_daily.csv".format(params['output_path'], natco)
    weekly_out_file = "{}/{}_weekly.csv".format(params['output_path'], natco)
    if (path.exists(daily_out_file) and kpis_path is not None and path.exists(weekly_out_file)):
        daily = pd.read_csv(daily_out_file, delimiter='|', header=0, dtype=str)
        weekly = pd.read_csv(weekly_out_file, delimiter='|', header=0, dtype=str)
        kpis = KPI_reader(params['kpis_path']).read_data()
        avg_proc = Weekly_avgs(dailyOutput=daily, weeklyOutput=weekly, kpis=kpis )
        result = avg_proc.process_data()
        avg_file = "{}/{}_weekly_averages_from_daily_input.csv".format(params['output_path'],natco)
        result['averages'].to_csv(avg_file, sep="|", index=False)
        result['weekly_out'].to_csv(weekly_out_file, sep="|", index=False)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    run_outputs_processing("COSGRE", 'weekly_input', params)
    #kpis = KPI_reader(params['kpis_path']).read_data()
    #run_matrix_processing("TMA", params)


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
