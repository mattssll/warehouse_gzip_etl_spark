"""DAG that builds a warehouse"""
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import date, datetime, timedelta
from dependencies.ApiCalls import APICalls

default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=30),
    'owner': 'airflow',
}

with DAG(
    'sparkPipe',
    default_args=default_args,
    schedule_interval='0 1 * * *', # once in a day for starters 1 am utc
    start_date=days_ago(2),
) as dag:


    endpoint_t1 = "http://127.0.0.1:5000/"
    t1 = PythonOperator(
        task_id='run_spark_file',
        python_callable=APICalls.urllib_sync_api_request,
        provide_context=True,
        op_kwargs={
            'url' : endpoint_t1
                },
        dag=dag)
    # [END howto_operator_python]

    # [START howto_operator_python_kwargs]
    def my_sleeping_function(random_base):
        """This is a function that will run within the DAG execution"""
        time.sleep(random_base)

    # Generate 5 sleeping tasks, sleeping from 0.0 to 0.4 seconds respectively
    for i in range(5):
        t2 = PythonOperator(
            task_id='sleep_for_' + str(i),
            python_callable=my_sleeping_function,
            op_kwargs={'random_base': float(i) / 10},
        )
        t1 >> t2
