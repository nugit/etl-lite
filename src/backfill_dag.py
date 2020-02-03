from datetime import datetime
from airflow import DAG
from airflow.operators.dagrun_operator import DagRunOrder
from airflow.operators.python_operator import PythonOperator
from airflow.operators.multi_dagrun import TriggerMultiDagRunOperator


def generate_dag_run():
    for i in range(100):
        yield DagRunOrder(payload={'index': i})


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2015, 6, 1),
}


dag = DAG('reindex_scheduler', schedule_interval=None, default_args=default_args)


ran_dags = TriggerMultiDagRunOperator(
    task_id='gen_target_dag_run',
    dag=dag,
    trigger_dag_id='example_target_dag',
    python_callable=generate_dag_run,
)

dag = DAG(
    dag_id='example_target_dag',
    schedule_interval=None,
    default_args={'start_date': datetime.utcnow(), 'owner': 'airflow'},
)


def run_this_func(dag_run, **kwargs):
   print("Chunk received: {}".format(dag_run.conf['index']))


chunk_handler = PythonOperator(
    task_id='chunk_handler',
    provide_context=True,
    python_callable=run_this_func,
    dag=dag
)
