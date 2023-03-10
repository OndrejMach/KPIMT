import os

from airflow import DAG
from datetime import datetime, timedelta
from kerberos_python_operator import KerberosPythonOperator
from airflow.operators.bash_operator import BashOperator
from os import path
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
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
    dag_id='SIT_PROD_RCSEU_DAILY_PYTHON',
    default_args=default_args,
    description='SIT_PROD_RCSEU_DAILY_PYTHON',
    start_date=datetime(2017, 3, 20),
    schedule_interval = '40 6 * * *',
    catchup=False)

########################## CONFIGURATION ##########################
#Edge node folders
edgeInputFolder='/data_ext/apps/sit/rcseu/input'
edgeOutputFolder='/data_ext/apps/sit/rcseu/output'
edgeLibFolder='/data_ext/apps/sit/rcseu/lib'
#edgeLandingZone='/data/input/ewhr/work/rcseu'
edgeLandingZone='/data_ext/input/sit/work/rcseu/'

#HDFS folders
hdfsOutputFolder='/data/sit/rcseu/output'
hdfsOutputArchiveFolder='/data/sit/rcseu/output-archive'
hdfsInputFolder='/data/sit/rcseu/input'
hdfsArchiveFolder='/data/sit/rcseu/archive'
hdfsStageFolder='/data/sit/rcseu/stage'

jobFolder='/data_ext/apps/sit/rcseu'
regular_processing_date = datetime.today() - timedelta(days=2)
update_processing_date = datetime.today() - timedelta(days=3)
missing_file_notification = ['ondrej.machacek@open-bean.com','sit-support@t-mobile.cz','a5f84da9.tmst365.onmicrosoft.com@emea.teams.ms']
pending_file_notification = ['ondrej.machacek@open-bean.com','sit-support@t-mobile.cz','a5f84da9.tmst365.onmicrosoft.com@emea.teams.ms']
natcos_to_check = [ 'td' , 'at']
stable_natcos=['cg', 'cr', 'mk', 'mt', 'st', 'tp', 'tc']
natcos_to_process = ['cg', 'cr', 'mk', 'mt', 'st', 'tp', 'tc','td', 'at']
#natcos_to_process = [ 'tp']
QS_remote = 'cdrs@10.105.180.206:/RCS-EU/PROD/'
########################## END CONFIGURATION ##########################

runDate=regular_processing_date.strftime('%Y-%m-%d')    #$(date -d "2 days ago" +'%Y-%m-%d')
outputDate= regular_processing_date.strftime('%Y%m%d')  #$(date -d "2 days ago" +'%Y%m%d')
outputMonth=regular_processing_date.strftime('%Y%m')  #$(date -d "2 days ago" +'%Y%m')
outputYear=regular_processing_date.strftime('%Y')   #$(date -d "2 days ago" +'%Y')


yesterdayDate=update_processing_date.strftime('%Y-%m-%d')   #$(date -d "3 days ago" +'%Y-%m-%d')
outputYesterday= update_processing_date.strftime('%Y%m%d')  #$(date -d "3 days ago" +'%Y%m%d')
outputYesterdayMonth=update_processing_date.strftime('%Y%m')   #$(date -d "3 days ago" +'%Y%m')

#Name of executable
appFile='ignite-1.0-all.jar'

spark_submit_template = ('/opt/cloudera/parcels/CDH/lib/spark/bin/spark-submit --master yarn --queue root.ewhr_technical ' 
'--deploy-mode cluster --num-executors 24 --executor-cores 8 --executor-memory 32G '
'--driver-memory 32G --conf spark.dynamicAllocation.enabled=false --conf spark.shuffle.memoryFraction=0 '
'--driver-java-options "-Dlog4j.configuration=file:/data_ext/apps/sit/rcseu/conf/log4j.custom.properties" '
'--class "com.tmobile.sit.ignite.rcseu.Application" {}/{} {} {} {}')


#hdfs_put_cmd = 'hdfs dfs -put -f {}/*{}*.gz {}'.format(edgeInputFolder, runDate, hdfsArchiveFolder)
hdfs_put_cmd = 'echo "Files archiving done"'
get_outputs_cmd = 'hdfs dfs -get -f {}/User_agents.csv {} && hdfs dfs -get -f {}/*.csv {}/'.format(hdfsStageFolder,edgeOutputFolder,hdfsOutputFolder,edgeOutputFolder)  # hdfs dfs -get -f $hdfsOutputFolder/activity*daily*${outputYesterday}.csv $edgeOutputFolder/
send_outputs_cmd = 'scp {}/*.csv {}'.format(edgeOutputFolder,QS_remote)  #scp $edgeOutputFolder/activity*daily*${outputYesterday}.csv cdrs@10.105.180.206:/RCS-EU/PROD/
### CLEANUP CMDs
archive_agents_cmd = 'hdfs dfs -cp -f {}/User_agents.csv {}/User_agents.{}.csv'.format(hdfsStageFolder,hdfsOutputArchiveFolder,runDate)
archive_outputs_cmd = 'hdfs dfs -cp -f {}/*.* {}/'.format(hdfsOutputFolder,hdfsOutputArchiveFolder )
delete_hdfs_output_cmd = 'hdfs dfs -rm {}/*.*'.format(hdfsOutputFolder)
delete_edge_output_cmd = 'rm {}/*.*'.format(edgeOutputFolder)
#delete_edge_input_cmd = 'rm {}/*.gz'.format(edgeInputFolder)

overall_cleanup_cmd = archive_agents_cmd + ' && ' + archive_outputs_cmd + ' && ' + delete_hdfs_output_cmd + ' && ' + delete_edge_output_cmd #+ ' && ' + delete_edge_input_cmd


def gzip_file(file):
    with open(file, 'rb') as f_in, gzip.open(file+'.gz', 'wb') as f_out:
        f_out.writelines(f_in)

def check_valid_input(**context):
    complete = True
    body = ""
    for natco in stable_natcos:
        for type in ['activity','provision','register_requests']:
            file = '{}/{}/{}_{}.csv_{}.csv'.format(edgeLandingZone, natco,type,runDate,natco)  #'$edgeLandingZone/tp/register_requests_2022-03-16.csv_mt.csv'
            if ( not path.exists(file)):
                complete = False
                body = body + type + ' file for ' + natco + " has not been delivered <br>"
    if (not complete):
        email_op = EmailOperator(
            task_id='send_email',
            to=missing_file_notification,
            subject="Input validity check for RCS-EU has FAILED",
            html_content=body,
            files=None,
        )
        email_op.execute(context)
        return "terminate"
    else:
        return "check_files"


def file_check(**context):
    for natco in natcos_to_check:
        for type in ['activity','provision','register_requests']:
            file = '{}/{}/{}_{}.csv_{}.csv'.format(edgeLandingZone, natco,type,runDate,natco)  #'$edgeLandingZone/tp/register_requests_2022-03-16.csv_mt.csv'
            if ( not path.exists(file)):
                empty_file = '{}/{}_empty.csv_tp.csv'.format(edgeLandingZone,type) #$edgeLandingZone/activity_empty.csv_tp.csv
                shutil.copyfile(empty_file, file)
                email_op = EmailOperator(
                    task_id='send_email',
                    to=missing_file_notification,
                    subject="Missing file report " + natco,
                    html_content=type + ' file for ' + natco + ' has not been delivered' ,
                    files=None,
                )
                email_op.execute(context)

def get_files(natco):
    pending_files = []
    for type in ['activity', 'provision', 'register_requests']:
        file = glob.glob('{}/{}/{}_*.csv_{}.csv'.format(edgeLandingZone, natco, type, natco))
        pending_files += file  # '$edgeLandingZone/tp/register_requests_2022-03-16.csv_mt.csv'
    if (len(pending_files) >0):
        pending_files.sort(key=os.path.getmtime)
    return pending_files

def check_pending(**context):
    pending_files = []
    for natco in natcos_to_process:
        pending_files += get_files(natco) #'$edgeLandingZone/tp/register_requests_2022-03-16.csv_mt.csv'

    print('found pending files: ' + ', '.join([str(x) for x in pending_files]))
    if ( pending_files):
        email_op = EmailOperator(
            task_id='send_email',
            to=pending_file_notification,
            subject="Pending files report ",
            html_content='pending files list: '+', '.join([str(x) for x in pending_files]) ,
            files=None,
        )
        email_op.execute(context)

def reprocess_pending(**context):
    natcos_reprocessed = 0
    for natco in natcos_to_process:
        pending_files = get_files(natco)
        dates = set()
        for file in pending_files:
            print("Reprocessing pending file: "+file)
            result = re.search(r"\w+_(\d+-\d+-\d+).csv", file)
            os.system("gzip {} && hdfs dfs -put -f {}.gz {} && rm {}.gz".format(file, file,hdfsArchiveFolder, file))
            dates.add(result.group(1))
        if (dates):
            dates_p = list(dates)
            dates_p.sort()
            natcos_reprocessed+=1
            for date in dates_p:
                print("running reprocessing for natco: " + natco + " date: " + date)
                date_update = datetime.strptime(date, '%Y-%m-%d') - timedelta(days=1)
                command = spark_submit_template.format(edgeLibFolder, appFile, date_update.strftime('%Y-%m-%d'), natco,'update')
                os.system(command)
                command = spark_submit_template.format(edgeLibFolder, appFile, date, natco,'daily')
                os.system(command)
    if (natcos_reprocessed>0):
        os.system(get_outputs_cmd)
        os.system(send_outputs_cmd)

def copy_files(**context):
    for natco in natcos_to_process:
        #$edgeLandingZone/${natco}/*$runDate*.* $edgeInputFolder/
        landing_files = '{}/{}/*{}*.csv'.format(edgeLandingZone,natco,runDate)
        filelist = glob.glob(landing_files)
        for single_file in filelist:
            # move file with full paths as shutil.move() parameters
            gzip_file(single_file)
            #shutil.move(single_file, edgeInputFolder+'/')
            put_to_hdfs_cmd = "hdfs dfs -put -f {} {}".format(single_file+".gz", hdfsArchiveFolder)
            os.system(put_to_hdfs_cmd)
            os.remove(single_file+".gz")
            os.remove(single_file)
        #filelistToGzip = glob.glob('{}/*{}*.csv'.format(edgeInputFolder,natco))
        #for input_file in filelistToGzip:
        #    gzip_file(input_file)

def run_daily_processing(**context):
    for natco in natcos_to_process:
        spark_submit_cmd = spark_submit_template.format(edgeLibFolder, appFile, runDate, natco,'daily')
        bash_op = BashOperator(task_id='run_spark',bash_command=spark_submit_cmd)
        bash_op.execute(context)

def run_update_processing(**context):
    for natco in natcos_to_process:
        spark_submit_cmd = spark_submit_template.format(edgeLibFolder, appFile, yesterdayDate, natco,'update')
        bash_op = BashOperator(task_id='run_spark',bash_command=spark_submit_cmd)
        bash_op.execute(context)

def run_yearly_processing(**context):
    for natco in natcos_to_process:
        spark_submit_cmd = spark_submit_template.format(edgeLibFolder, appFile, runDate, natco,'yearly')
        bash_op = BashOperator(task_id='run_spark',bash_command=spark_submit_cmd)
        bash_op.execute(context)

#hdfs dfs -get -f $hdfsStageFolder/User_agents.csv $edgeOutputFolder/



check_files = KerberosPythonOperator(
    task_id='check_files',
    python_callable=file_check,
    dag=dag
)

check_pending_files = KerberosPythonOperator(
    task_id='check_pending_files',
    python_callable=check_pending,
    dag=dag
)


copy_files_to_input = KerberosPythonOperator(
    task_id='copy_files',
    python_callable=copy_files,
    dag=dag
)

put_to_archive = BashOperator(task_id='put_to_archive', bash_command=hdfs_put_cmd, dag=dag)

daily_processing = KerberosPythonOperator(
    task_id='run_daily_processing',
    python_callable=run_daily_processing,
    dag=dag
)

yearly_processing = KerberosPythonOperator(
    task_id='run_yearly_processing',
    python_callable=run_yearly_processing,
    dag=dag
)

update_processing = KerberosPythonOperator(
    task_id='run_update_processing',
    python_callable=run_update_processing,
    dag=dag
)

process_pending_files = KerberosPythonOperator(
    task_id='reprocess_pending',
    python_callable=reprocess_pending,
    dag=dag
)

input_validity_check = BranchPythonOperator(
    task_id='input_validity_check',
    python_callable=check_valid_input,
    dag=dag,
)



get_outputs = BashOperator(task_id='get_outputs', bash_command=get_outputs_cmd, dag=dag)
send_outputs = BashOperator(task_id='send_outputs', bash_command=send_outputs_cmd, dag=dag)
cleanup = BashOperator(task_id='cleanup', bash_command=overall_cleanup_cmd, dag=dag)
terminate = DummyOperator(task_id='terminate', dag=dag)

input_validity_check >> terminate
input_validity_check >> check_files >> copy_files_to_input >> put_to_archive >> update_processing >> daily_processing >> yearly_processing >>get_outputs >> send_outputs >>cleanup >> check_pending_files >> process_pending_files


#get_source_files = KerberosBashOperator(task_id='get_source_files', bash_command='/data_ext/apps/sit/rcseu/0-PutToHDFS.sh ', dag=dag)
#process_daily = KerberosBashOperator(task_id='process_daily', bash_command='/data_ext/apps/sit/rcseu/2-ProcessNatcosDaily.sh ', dag=dag)
#send_daily_results = KerberosBashOperator(task_id='send_daily_results', bash_command='/data_ext/apps/sit/rcseu/3-GetOutputAndSendDaily.sh ', dag=dag)
#process_yearly = KerberosBashOperator(task_id='process_yearly', bash_command='/data_ext/apps/sit/rcseu/4-ProcessNatcosYearly.sh ', dag=dag)
#send_yearly_results = KerberosBashOperator(task_id='send_yearly_results', bash_command='/data_ext/apps/sit/rcseu/5-GetOutputAndSendYearly.sh ', dag=dag)
#process_updates = KerberosBashOperator(task_id='process_updates', bash_command='/data_ext/apps/sit/rcseu/6-UpdateNatcosYesterday.sh ', dag=dag)
#send_updates = KerberosBashOperator(task_id='send_updates', bash_command='/data_ext/apps/sit/rcseu/7-GetUpdateAndSendYesterday.sh ', dag=dag)
#archive_cleanup = KerberosBashOperator(task_id='archive_cleanup', bash_command='/data_ext/apps/sit/rcseu/8-CleanupAndArchive.sh ', dag=dag)


