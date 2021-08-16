from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from kerberos_python_operator import KerberosPythonOperator
from datetime import datetime
from sit.classes.SFTP_handler import SFTP_handler

from sit.kpimt.run_processing import run_outputs_processing, run_avg_processing, run_matrix_processing, run_multimarket, run_ims

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
    schedule_interval = '05 7,11,15 * * *',
    catchup=False)

params = {
    "kpis_path": "/data_ext/apps/sit/kpimt/kpi_request/",
    "correnctions_path": "/data_ext/apps/sit/kpimt/correction/",
    'basepath': "/data_ext/apps/sit/kpimt/input/",
    "output_path": "/data_ext/apps/sit/kpimt/output/",
    "archive_path_output": "/data_ext/apps/sit/kpimt/archive/output/",
    "archive_path_input": "/data_ext/apps/sit/kpimt/archive/input/",
    'multimarket': "/data_ext/apps/sit/kpimt/input/multi_market/",
    'multimarket_archive': "/data_ext/apps/sit/kpimt/archive/input/multi_market/",
    'ims_path' : "/data_ext/apps/sit/kpimt/input/IMS/",
    'ims_archive': "/data_ext/apps/sit/kpimt/archive/input/IMS/"
}
files_to_deliver = ["Corrections.csv","COSGRE_Matrix_monthly.csv","COSGRE_Matrix_weekly.csv",
                    "COSROM_Matrix_monthly.csv","COSROM_Matrix_weekly.csv",
                    "IMS_facts.csv","TMA_Matrix_daily.csv","TMA_Matrix_monthly.csv","TMA_Matrix_weekly.csv",
                    "TMA_monthly_averages_from_daily_input.csv","TMCG_Matrix_monthly.csv","TMCG_Matrix_weekly.csv",
                    "TMCZ_Matrix_daily.csv","TMCZ_Matrix_monthly.csv","TMCZ_Matrix_weekly.csv","TMCZ_monthly_averages_from_daily_input.csv",
                    "TMD_Matrix_monthly.csv","TMD_Matrix_weekly.csv","TMHR_Matrix_monthly.csv","TMHR_Matrix_weekly.csv",
                    "TMHU_Matrix_monthly.csv","TMHU_Matrix_weekly.csv","TMMK_Matrix_monthly.csv","TMMK_Matrix_weekly.csv",
                    "TMNL_Matrix_daily.csv","TMNL_Matrix_monthly.csv","TMNL_Matrix_weekly.csv","TMNL_monthly_averages_from_daily_input.csv",
                    "TMPL_Matrix_daily.csv","TMPL_Matrix_monthly.csv","TMPL_Matrix_weekly.csv","TMPL_monthly_averages_from_daily_input.csv",
                    "TMSK_Matrix_monthly.csv","TMSK_Matrix_weekly.csv","DTAG-KPI-formular_Report_Mapping_database_master.xlsx","DTAG-KPI-formular_database-master.xlsx"]


qs_server_ip='10.105.180.206'
qs_server_user='cdrs'
qs_server_folder='/IntKPIMonitoring'
cdrs_sftp_key='/home/talend_ewhr/.ssh/id_rsa'


natco_list=["COSGRE", "COSROM", "TMA", "TMCG", "TMCZ", "TMD", "TMHR", "TMHU", "TMMK", "TMNL", "TMPL", "TMSK"]
sftp_out = SFTP_handler(host=qs_server_ip,private_key=cdrs_sftp_key ,chdir=qs_server_folder,username=qs_server_user)
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

def multimarket_processing():
    run_multimarket(params=params)

def ims_processing():
    run_ims(params=params)

def upload_qs():
    sftp_out.connect()
    local_path = params['output_path']
    matrix_path = local_path + "Matrix/"
    kpis_path = params['kpis_path']
    corrections_path = params['correnctions_path']
    sftp_out.upload(local_folder=local_path, file_filter=files_to_deliver)
    sftp_out.upload(local_folder=matrix_path, file_filter=files_to_deliver)
    sftp_out.upload(local_folder=kpis_path, file_filter=files_to_deliver)
    sftp_out.upload(local_folder=corrections_path, file_filter=files_to_deliver)
    sftp_out.close()


def matrix_processing():
    for natco in natco_list:
        print("running matrix processing")
        run_matrix_processing(natco=natco, params=params)


process_files = KerberosPythonOperator(
        task_id='process_files',
        python_callable=run_processing,
        dag=dag
    )

process_multimarket = KerberosPythonOperator(
        task_id='process_multimarket',
        python_callable=multimarket_processing,
        dag=dag
    )

process_matrices = KerberosPythonOperator(
        task_id='matrix_processing',
        python_callable=matrix_processing,
        dag=dag
    )

process_ims = KerberosPythonOperator(
        task_id='process_ims',
        python_callable=ims_processing,
        dag=dag
    )

upload_results = KerberosPythonOperator(
        task_id='upload_results',
        python_callable=upload_qs,
        dag=dag
    )

input_archive = BashOperator(task_id='archive_input', bash_command=input_backup, dag=dag)
output_archive = BashOperator(task_id='archive_output', bash_command=backup_command, dag=dag)


input_archive >> output_archive >> process_files >> process_multimarket >>process_matrices >> process_ims >>upload_results