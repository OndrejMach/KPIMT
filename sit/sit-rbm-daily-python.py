import os

from airflow import DAG
from datetime import datetime, timedelta
from kerberos_python_operator import KerberosPythonOperator
from airflow.operators.bash_operator import BashOperator
from os import path
from airflow.operators.email_operator import EmailOperator
import shutil
import glob
import gzip
import re


default_args = {
    'queue': 'SIT_Queue',
    'owner': 'kr_prod_airflow_operation_ewhr',
    'run_as_user': 'talend_ewhr',
    'start_date': datetime(2020, 2, 18),
    'retries': 0,
    'email': ['ondrej.machacek@open-bean.com', 'ondrej.machacek@external.t-mobile.cz','sit-support@t-mobile.cz','a5f84da9.tmst365.onmicrosoft.com@emea.teams.ms'], #'q6o7a8w0b9u9x3b4@sit-cz.slack.com'
    'email_on_failure': True
}

dag = DAG(
    dag_id='SIT_PROD_RBM_DAILY_PYTHON',
    default_args=default_args,
    description='SIT_PROD_RBM_DAILY_PYTHON',
    start_date=datetime(2017, 3, 20),
    schedule_interval = '40 6 * * *',
    catchup=False)

########################## CONFIGURATION ##########################
#Edge node folders
edgeInputFolder='/data_ext/apps/sit/rbm/input'
edgeOutputFolder='/data_ext/apps/sit/rbm/output'
edgeLibFolder='/data_ext/apps/sit/rbm/lib'
#edgeLandingZone='/data/input/ewhr/work/rcseu'
edgeLandingZone='/data_ext/input/sit/work/rbm/'

#HDFS folders
hdfsOutputFolder='/data/sit/rbm/output'
hdfsInputArchiveFolder='/data/sit/rbm/input_archive'
hdfsInputFolder='/data/sit/rbm/input'
hdfsArchiveFolder='/data/sit/rbm/archive'
hdfsStageFolder='/data/sit/rbm/stage'

missing_file_notification = ['ondrej.machacek@open-bean.com','sit-support@t-mobile.cz','a5f84da9.tmst365.onmicrosoft.com@emea.teams.ms']
jobFolder='/data_ext/apps/sit/rbm'
regular_processing_date = datetime.today() - timedelta(days=2)
natcos_to_process = ["cg",   "mk",   "st",  "tc",  "tp", "td"]
#natcos_to_process = [ 'tp']
QS_remote = 'cdrs@10.105.180.206:/RBM/'
########################## END CONFIGURATION ##########################

runDate=regular_processing_date.strftime('%Y-%m-%d')    #$(date -d "2 days ago" +'%Y-%m-%d')
outputDate= regular_processing_date.strftime('%Y%m%d')  #$(date -d "2 days ago" +'%Y%m%d')
outputMonth=regular_processing_date.strftime('%Y%m')  #$(date -d "2 days ago" +'%Y%m')
outputYear=regular_processing_date.strftime('%Y')   #$(date -d "2 days ago" +'%Y')


#yesterdayDate=update_processing_date.strftime('%Y-%m-%d')   #$(date -d "3 days ago" +'%Y-%m-%d')
#outputYesterday= update_processing_date.strftime('%Y%m%d')  #$(date -d "3 days ago" +'%Y%m%d')
#outputYesterdayMonth=update_processing_date.strftime('%Y%m')   #$(date -d "3 days ago" +'%Y%m')

#Name of executable
appFile='rbm-ignite-1.0-all.jar'

spark_submit_template = ('/opt/cloudera/parcels/CDH/lib/spark/bin/spark-submit --master yarn --queue root.ewhr_technical ' 
'--deploy-mode cluster --num-executors 24 --executor-cores 8 \--executor-memory 20G '
'--driver-memory 20G --conf spark.dynamicAllocation.enabled=false '
'--driver-java-options "-Dlog4j.configuration=file:/data_ext/apps/sit/rcseu/conf/log4j.custom.properties" '
'--class "com.tmobile.sit.rbm.Processor" {}/{} -natco={} -date={}')
#-natco=cg -date=2022-05-29

#hdfs_put_cmd = 'hdfs dfs -put -f {}/*{}*.gz {}'.format(edgeInputFolder, runDate, hdfsArchiveFolder)
hdfs_put_cmd = 'echo "Files archiving done"'
get_outputs_cmd = 'hdfs dfs -get -f {}/*.csv {}/'.format(hdfsOutputFolder,edgeOutputFolder)  # hdfs dfs -get -f $hdfsOutputFolder/activity*daily*${outputYesterday}.csv $edgeOutputFolder/
send_outputs_cmd = 'scp {}/*.csv {}'.format(edgeOutputFolder,QS_remote)  #scp $edgeOutputFolder/activity*daily*${outputYesterday}.csv cdrs@10.105.180.206:/RCS-EU/PROD/
### CLEANUP CMDs
#archive_agents_cmd = 'hdfs dfs -cp -f {}/User_agents.csv {}/User_agents.{}.csv'.format(hdfsStageFolder,hdfsOutputArchiveFolder,runDate)
#archive_outputs_cmd = 'hdfs dfs -cp -f {}/*.* {}/'.format(hdfsOutputFolder,hdfsOutputArchiveFolder )
#delete_hdfs_output_cmd = 'hdfs dfs -rm {}/*.*'.format(hdfsOutputFolder)
delete_edge_output_cmd = 'rm {}/*.*'.format(edgeOutputFolder)
#delete_edge_input_cmd = 'rm {}/*.gz'.format(edgeInputFolder)

#overall_cleanup_cmd = archive_agents_cmd + ' && ' + archive_outputs_cmd + ' && ' + delete_hdfs_output_cmd + ' && ' + delete_edge_output_cmd #+ ' && ' + delete_edge_input_cmd


def gzip_file(file):
    with open(file, 'rb') as f_in, gzip.open(file+'.gz', 'wb') as f_out:
        f_out.writelines(f_in)

#def backfill_file(processing_date, natco):
#    files = []
#    # rbm_billable_events_2022-05-29.csv_cg.csv, rbm_activity_2022-12-24.csv_st.csv
#    for type in ['rbm_activity', 'rbm_billable_events']:
#        file = glob.glob('{}/{}/{}_{}.csv_{}.csv'.format(edgeLandingZone, natco, type, processing_date, natco))
#        files += file  # '$edgeLandingZone/tp/register_requests_2022-03-16.csv_mt.csv'
#    if (len(files) == 1):
#        f= ""
#        if ("rbm_activity" in files[0]):
#            print("INFO: Billable file missing for date: {},natco: {}  adding an empty one".format(processing_date, natco))
#            f = "{}/{}/rbm_billable_events_{}.csv_{}.csv.gz".format(edgeLandingZone, natco,processing_date,natco)
#            shutil.copy("{}/empty_billable.gz".format(jobFolder),f)
#        if ("rbm_billable_events" in files[0]):
#            print("INFO: Activity file missing for date: {},natco: {}  adding an empty one".format(processing_date,
#                                                                                                   natco))
#            f="{}/{}/rbm_activity_{}.csv_{}.csv.gz".format(edgeLandingZone, natco, processing_date,
#                                                                         natco)
#            shutil.copy("{}/empty_activity.gz".format(jobFolder),f)
#x         os.system("hdfs dfs -put -f {} {} && rm {}".format(f, hdfsInputArchiveFolder, f))

def get_files(natco):
    pending_files = []
    result = {}
    #rbm_billable_events_2022-05-29.csv_cg.csv, rbm_activity_2022-12-24.csv_st.csv
    for type in ['rbm_activity', 'rbm_billable_events']:
        file = glob.glob('{}/{}/{}_*.csv_{}.csv'.format(edgeLandingZone, natco, type ,natco))
        pending_files += file  # '$edgeLandingZone/tp/register_requests_2022-03-16.csv_mt.csv'
    for file in pending_files:
        date_search = re.search(r"\w+_(\d+-\d+-\d+).csv", file)
        date = date_search.group(1)
        if (date in result.keys()):
            result[date].append(file)
        else:
            result[date] = [file]
    return result


def process_files(**context):
    natcos_reprocessed = 0
    incomplete_delivery = []
    for natco in natcos_to_process:
        pending_files = get_files(natco )
        dates = set()
        for date in pending_files.keys():
            if (len(pending_files[date]) <2):
                incomplete_delivery.append(pending_files[date][0])
            else:
                dates.add(date)
                for file in pending_files[date]:
                    os.system("gzip {} && hdfs dfs -put -f {}.gz {} && rm {}.gz".format(file, file,hdfsInputArchiveFolder, file))
        if (dates):
            natcos_reprocessed+=1
            for date in dates:
                print("running processing for natco: " + natco + " date: " + date)
                command = spark_submit_template.format(edgeLibFolder, appFile, natco,date)
                os.system(command)
    if (natcos_reprocessed>0):
        os.system(get_outputs_cmd)
        os.system(send_outputs_cmd)
    if (len(incomplete_delivery) > 0):
        file_list = ""
        for i in incomplete_delivery:
            file_list = file_list + i + "<br>"
        email_op = EmailOperator(
            task_id='send_email',
            to=missing_file_notification,
            subject="Missing file report for RBM",
            html_content='Incomplete delivery for files (only delivered):<br> '+file_list,
            files=None,
        )
        email_op.execute(context)


process_files = KerberosPythonOperator(
    task_id='process_files',
    python_callable=process_files,
    dag=dag
)


get_outputs = BashOperator(task_id='get_outputs', bash_command=get_outputs_cmd, dag=dag)
send_outputs = BashOperator(task_id='send_outputs', bash_command=send_outputs_cmd, dag=dag)
cleanup = BashOperator(task_id='cleanup', bash_command=delete_edge_output_cmd, dag=dag)


process_files >> get_outputs >> send_outputs >> cleanup

#get_source_files = KerberosBashOperator(task_id='get_source_files', bash_command='/data_ext/apps/sit/rcseu/0-PutToHDFS.sh ', dag=dag)
#process_daily = KerberosBashOperator(task_id='process_daily', bash_command='/data_ext/apps/sit/rcseu/2-ProcessNatcosDaily.sh ', dag=dag)
#send_daily_results = KerberosBashOperator(task_id='send_daily_results', bash_command='/data_ext/apps/sit/rcseu/3-GetOutputAndSendDaily.sh ', dag=dag)
#process_yearly = KerberosBashOperator(task_id='process_yearly', bash_command='/data_ext/apps/sit/rcseu/4-ProcessNatcosYearly.sh ', dag=dag)
#send_yearly_results = KerberosBashOperator(task_id='send_yearly_results', bash_command='/data_ext/apps/sit/rcseu/5-GetOutputAndSendYearly.sh ', dag=dag)
#process_updates = KerberosBashOperator(task_id='process_updates', bash_command='/data_ext/apps/sit/rcseu/6-UpdateNatcosYesterday.sh ', dag=dag)
#send_updates = KerberosBashOperator(task_id='send_updates', bash_command='/data_ext/apps/sit/rcseu/7-GetUpdateAndSendYesterday.sh ', dag=dag)
#archive_cleanup = KerberosBashOperator(task_id='archive_cleanup', bash_command='/data_ext/apps/sit/rcseu/8-CleanupAndArchive.sh ', dag=dag)


