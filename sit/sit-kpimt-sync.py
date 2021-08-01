from airflow import DAG
from datetime import datetime
from kerberos_python_operator import KerberosPythonOperator

from sit.classes.SFTP_handler import SFTP_handler
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
    dag_id='SIT_PROD_KPIMT_SYNC_PYTHON',
    default_args=default_args,
    description='SIT_PROD_KPIMT_SYNC_PYTHON',
    start_date=datetime(2017, 3, 20),
    schedule_interval = '00 12 * * *',
    catchup=False)

#Edge node folders
edgeInputFolder='/data_ext/apps/sit/kpimt/input'
edgeOutputFolder='/data_ext/apps/sit/kpimt/output'
edgeLibFolder='/data_ext/apps/sit/kpimt/lib'

#App location
appFolder='/data_ext/apps/sit/kpimt'
#Name of executable
appFile='kpimt-1.0-all.jar'


#SFTP key used in all connections
cdrs_sftp_key='/home/talend_ewhr/.ssh/id_rsa'

#landing zone sftp (sit proxy)
sit_proxy_ip='10.105.240.221'
sit_proxy_user='cdrs_source'
sit_proxy_folder='/IntKPIMonitoring'

#QV forwarding (old solution)
qv_server_ip='10.105.252.96'
qv_server_user='cdrs'
qv_server_folder='/'

#QS server (new solution)
qs_server_ip='10.105.180.206'
qs_server_user='cdrs'
qs_server_folder='/IntKPIMonitoring'

natco_list=["COSGRE", "COSROM", "TMA", "TMCG", "TMCZ", "TMD", "TMHR", "TMHU", "TMMK", "TMNL", "TMPL", "TMSK"]

sftp_in = SFTP_handler(host=sit_proxy_ip, private_key=cdrs_sftp_key ,chdir=sit_proxy_folder,username=sit_proxy_user)
sftp_out = SFTP_handler(host=qv_server_ip,private_key=cdrs_sftp_key ,chdir=qv_server_folder,username=qv_server_user)

#tmpFile = sftp_in.getTmpFile(app_tmp)

def handle_multimarket():
    sftp_in.connect()
    remote_path = "{}/multi_market/".format(sit_proxy_folder)
    local_path = "{}/multi_market/".format(edgeInputFolder)
    if (sftp_in.set_chdir(remote_path)):
        sftp_in.get(local_path=local_path)
    sftp_out.connect()
    qv_path = "/multi_market/monthly_input/"
    if (sftp_out.set_chdir(qv_path)):
        sftp_out.upload(local_folder=local_path)
    #if (sftp_in.set_chdir(remote_path)):
        #sftp_in.delete()
    sftp_in.close()
    sftp_out.close()

def handle_ims():
    sftp_in.connect()
    remote_path = "{}/IMS/".format(sit_proxy_folder)
    local_path = "{}/IMS/".format(edgeInputFolder)
    if (sftp_in.set_chdir(remote_path)):
        sftp_in.get(local_path=local_path)
    sftp_out.connect()
    qv_path = "/IMS/Performance_Data/"
    if (sftp_out.set_chdir(qv_path)):
        sftp_out.upload(local_folder=local_path)
    # if (sftp_in.set_chdir(remote_path)):
        # sftp_in.delete()
    sftp_in.close()
    sftp_out.close()

def handle_kpi_request():
    sftp_in.connect()
    remote_path = "{}/other_files/".format(sit_proxy_folder)
    local_path = "{}/other_files/".format(edgeInputFolder)
    if(sftp_in.set_chdir(remote_path)):
        sftp_in.get(local_path=local_path)
    sftp_out.connect()
    qv_path = "/KPIRequest/"
    if(sftp_out.set_chdir(qv_path)):
        sftp_out.upload(local_folder=local_path)
    # if (sftp_in.set_chdir(remote_path)):
        # sftp_in.delete()
    sftp_in.close()
    sftp_out.close()

def download_files_sftp():
    sftp_in.connect()
    for natco in natco_list:
        remote_path = "{}/{}/daily/".format(sit_proxy_folder,natco)
        local_path = "{}/{}/daily/".format(edgeInputFolder,natco)
        if (sftp_in.set_chdir(remote_path)):
            sftp_in.get(local_path=local_path)
        remote_path = "{}/{}/weekly/".format(sit_proxy_folder, natco)
        local_path = "{}/{}/weekly/".format(edgeInputFolder, natco)
        if (sftp_in.set_chdir(remote_path)):
            sftp_in.get(local_path=local_path)
        remote_path = "{}/{}/monthly/".format(sit_proxy_folder, natco)
        local_path = "{}/{}/monthly/".format(edgeInputFolder, natco)
        if (sftp_in.set_chdir(remote_path)):
            sftp_in.get(local_path=local_path)
    sftp_in.close()

def delete_files_sftp():
    sftp_in.connect()
    for natco in natco_list:
        remote_path = "{}/{}/daily/".format(sit_proxy_folder,natco)
        if (sftp_in.set_chdir(remote_path)):
            sftp_in.delete()
        remote_path = "{}/{}/weekly/".format(sit_proxy_folder, natco)
        if (sftp_in.set_chdir(remote_path)):
            sftp_in.delete()
        remote_path = "{}/{}/monthly/".format(sit_proxy_folder, natco)
        if (sftp_in.set_chdir(remote_path)):
            sftp_in.delete()
    sftp_in.close()


def upload_sftp():
    sftp_out.connect()
    for natco in natco_list:
        remote_path = "/{}/daily_DWH_feed/".format(natco)
        local_path = "{}/{}/daily/".format(edgeInputFolder,natco)
        if (sftp_out.set_chdir(remote_path)):
            sftp_out.upload(local_folder=local_path)
        remote_path = "/{}/weekly_input/".format(natco)
        local_path = "{}/{}/weekly/".format(edgeInputFolder, natco)
        if (sftp_out.set_chdir(remote_path)):
            sftp_out.upload(local_folder=local_path)
        remote_path = "/{}/monthly_input/".format(natco)
        local_path = "{}/{}/monthly/".format(edgeInputFolder, natco)
        if (sftp_out.set_chdir(remote_path)):
            sftp_out.upload(local_folder=local_path)
    sftp_out.close()


deliver_files = KerberosPythonOperator(
        task_id='upload_sftp',
        python_callable=upload_sftp,
        dag=dag
    )

download_files = KerberosPythonOperator(
        task_id='download_files_sftp',
        python_callable=download_files_sftp,
        dag=dag
    )

#delete_files = KerberosPythonOperator(
#    task_id='delete_files_sftp',
#    python_callable=delete_files_sftp,
#    dag=dag,
#)

multi_market = KerberosPythonOperator(
    task_id='handle_multimarket',
    python_callable=handle_multimarket,
    dag=dag,
)

ims = KerberosPythonOperator(
    task_id='handle_ims',
    python_callable=handle_ims,
    dag=dag,
)

kpi_requests = KerberosPythonOperator(
    task_id='handle_kpi_request',
    python_callable=handle_kpi_request,
    dag=dag,
)

download_files >> deliver_files >> multi_market >> ims >> kpi_requests #'delete_files' shoule be added