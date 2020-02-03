from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from clients.facebook_ads import extract_data_1, extract_data_2

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'myarg1': 101,  # testing
    'myarg2': 202,  # testing
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'adhoc':False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'trigger_rule': u'all_success'
}

facebook_ads_dag = DAG(
    'facebook_ads_dag',
    default_args=default_args,
    description='Facebook Ads DAG',
    is_paused_upon_creation=True,
    concurrency=1,
    schedule_interval=None,
)

with facebook_ads_dag:
    fb_t1 = PythonOperator(
        task_id="facebook_ads_step_1",
        python_callable=extract_data_1,
        templates_dict=default_args,
        provide_context=True,
    )

    fb_t2 = PythonOperator(
        task_id="facebook_ads_step_2",
        python_callable=extract_data_2,
        templates_dict=default_args,
        provide_context=True,
    )

# fb_t1 >> fb_t2
