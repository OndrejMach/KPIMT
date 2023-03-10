from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from kerberos_python_operator import KerberosPythonOperator
from datetime import datetime, timedelta
from sit.classes.SFTP_handler import SFTP_handler
from airflow.operators.email_operator import EmailOperator
from sit.kpimt.run_processing import generate_report
from os import listdir
from os.path import isfile, join


default_args = {
    'owner': 'kr_prod_airflow_operation_ewhr',
    'run_as_user': 'talend_ewhr',
    'start_date': datetime(2020, 2, 18),
    'retries': 0,
    'email': ['ondrej.machacek@external.t-mobile.cz', 'q6o7a8w0b9u9x3b4@sit-cz.slack.com'],
    'email_on_failure': True
}

dag = DAG(
    dag_id='SIT_PROD_KPIMT_EMAIL_REPORT',
    default_args=default_args,
    description='SIT_PROD_KPIMT_EMAIL_REPORT',
    start_date=datetime(2017, 3, 20),
    schedule_interval='00 8 7,15 * *',
    catchup=False)

params = {
    "kpis_path": "/data_ext/apps/sit/kpimt/input/other_files/",
    "matrix_path": "/data_ext/apps/sit/kpimt/output/Matrix/",
    'reports_path': '/data_ext/apps/sit/kpimt/output/reports/',
    'reports_archive' : '/data_ext/apps/sit/kpimt/archive/reports/'
}

send_to = {
    #"all" : ['ondrej.machacek@external.t-mobile.cz', 'MKrebs@telekom.de'],
    "TMA"  : ['ondrej.machacek@external.t-mobile.cz','MKrebs@telekom.de',  'Karl.Hevera@magenta.at'],
    "TMCG"  : ['ondrej.machacek@external.t-mobile.cz','MKrebs@telekom.de',  'nela.slavic@telekom.me'],
    "TMCZ" : ['ondrej.machacek@external.t-mobile.cz', 'MKrebs@telekom.de', 'pavel.hornicek@t-mobile.cz'],
    "COSGRE" : ['ondrej.machacek@external.t-mobile.cz', 'MKrebs@telekom.de', 'fkyriazid@cosmote.gr'],
    "TMHR" : ['ondrej.machacek@external.t-mobile.cz','MKrebs@telekom.de',  'krunoslav.smoljak@t.ht.hr'],
    "TMHU" : ['ondrej.machacek@external.t-mobile.cz', 'MKrebs@telekom.de', 'kenesei.tibor@telekom.hu'],
    "TMMK" : ['ondrej.machacek@external.t-mobile.cz','MKrebs@telekom.de',  'Marko.Luevski@telekom.mk'],
    "TMNL" : ['ondrej.machacek@external.t-mobile.cz','MKrebs@telekom.de',  'Joost-Jelle.Oudemans@t-mobile.nl'],
    "TMPL" : ['ondrej.machacek@external.t-mobile.cz', 'MKrebs@telekom.de', 'Adam.Stryjewski@external.t-mobile.pl'],
    "COSROM" : ['ondrej.machacek@external.t-mobile.cz', 'MKrebs@telekom.de', 'tiberiu.lica@telekom.ro'],
    "TMSK" : ['ondrej.machacek@external.t-mobile.cz', 'MKrebs@telekom.de', 'ladislav.manko@telekom.sk']
}


timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

bash_command = 'cd {} && tar cvfz reports_{}.tar.gz * && mv *.tar.gz {} && rm *'.format(params['reports_path'], timestamp, params['reports_archive'])

def create_report():
    generate_report(kpis_path=params['kpis_path'], matrix_path=params['matrix_path'],
                    reports_path=params['reports_path'])


def build_email(**context):
    onlyfiles = [join(params['reports_path'], f) for f in listdir(params['reports_path']) if isfile(join(params['reports_path'], f))]
    for natco in send_to:
        files = [f for f in onlyfiles if natco in f]
        if (len(files) >0):
            email_op = EmailOperator(
                task_id='send_email',
                to=send_to[natco],
                subject="KPI REPORT for "+natco,
                html_content=None,
                files=files,
            )
            email_op.execute(context)


do_report = KerberosPythonOperator(
    task_id='create_report',
    python_callable=create_report,
    dag=dag
)
send_email = KerberosPythonOperator(
    task_id='send_email',
    python_callable=build_email,
    dag=dag
)

archive_reports = BashOperator(task_id='archive_reports', bash_command=bash_command, dag=dag)

do_report >> send_email >> archive_reports
