from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from kerberos_python_operator import KerberosPythonOperator
from datetime import datetime

from sit.kpimt.run_processing import run_outputs_processing, run_avg_processing, run_matrix_processing

#from sit.classes.SFTP_handler import SFTP_handler

default_args = {
    'owner': 'kr_prod_airflow_operation_ewhr',
    'run_as_user': 'talend_ewhr',
    'start_date': datetime(2020, 2, 18),
    'retries': 0,
    'email': ['ondrej.machacek@external.t-mobile.cz','q6o7a8w0b9u9x3b4@sit-cz.slack.com'],
    'email_on_failure': True
}

dag = DAG(
    dag_id='SIT_PROD_KPIMT_OUTPUTS_MATRIX',
    default_args=default_args,
    description='SIT_PROD_KPIMT_OUTPUTS_MATRIX',
    start_date=datetime(2017, 3, 20),
    schedule_interval = '00 13 * * *',
    catchup=False)

params = {
    "kpis_path": "/data_ext/apps/sit/kpimt/kpi_request/",
    "correnctions_path": "/data_ext/apps/sit/kpimt/correction/",
    'basepath': "/data_ext/apps/sit/kpimt/input/",
    "output_path": "/data_ext/apps/sit/kpimt/output/",
    "archive_path_output": "/data_ext/apps/sit/kpimt/archive/output/",
    "archive_path_input": "/data_ext/apps/sit/kpimt/archive/input/"
}



natco_list=["COSGRE", "COSROM", "TMA", "TMCG", "TMCZ", "TMD", "TMHR", "TMHU", "TMMK", "TMNL", "TMPL", "TMSK"]

#tmpFile = sftp_in.getTmpFile(app_tmp)
timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

backup_output_file= "output_"+timestamp+".tar.gz"
backup_command = "cd {} && tar cvfz {} * && mv {} {}".format(params['output_path'],backup_output_file,backup_output_file,params['archive_path_output'])
input_backup = "cp -r {}* {}".format(params['basepath'], params['archive_path_input'])

def run_processing():
    for natco in natco_list:
        print("processing natco: "+natco)
        daily_files_processed = run_outputs_processing(mode="daily_input", natco=natco, params=params)
        if (daily_files_processed >0):
            run_avg_processing(natco=natco, params=params, mode="weekly")
            run_avg_processing(natco=natco, params=params, mode="monthly")
        weekly_files_procesed = run_outputs_processing(mode="weekly_input", natco=natco, params=params)
        monthly_files_processed = run_outputs_processing(mode="monthly_input", natco=natco, params=params)
        total_files_processed = daily_files_processed + weekly_files_procesed + monthly_files_processed
        if (total_files_processed >0):
            print("running matrix processing")
            run_matrix_processing(natco=natco, params=params)
        else:
            print("NO INPUT FILES TO PROCESS, SKIPPING NATCO: " + natco)


process_files = KerberosPythonOperator(
        task_id='process_files',
        python_callable=run_processing,
        dag=dag
    )

input_archive = BashOperator(task_id='archive_input', bash_command=input_backup, dag=dag)
output_archive = BashOperator(task_id='archive_output', bash_command=backup_command, dag=dag)


input_archive >> output_archive >> process_files