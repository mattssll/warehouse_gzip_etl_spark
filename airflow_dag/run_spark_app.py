"""DAG that builds our solution"""
from airflow import DAG
import time
from datetime import date, datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import sys
sys.path.append("/Users/mateus.leao/Documents/mattssll/takeaway/")
from app_spark.download import download_file # one of the methods I defined
from app_viz_report.pandas_charts import generate_viz_report
from airflow.utils.dates import days_ago

absolutePathToApp = "/Users/mateus.leao/Documents/mattssll/takeaway"
default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'owner': 'airflow',
    'email': "yoursenderemailhere@gmail.com", # this has to be configured w send grid for ej.
    'email_on_failure': True,
    'email_on_retry': True,
}

splitSizeJsonGz = 300000

with DAG(
    'sparkPipe',
    default_args=default_args,
    schedule_interval='1 1,9,15 * * *', # running 3 times a day at hour 1,9, and 15 UTC
    catchup=False,
    start_date=days_ago(2),
    is_paused_upon_creation=False
) as dag:


    t1 = BashOperator(
        task_id='remove_metadata.json.gz_if_it_exists',
        depends_on_past=False,
        bash_command=f'rm {absolutePathToApp}/input_data/metadata.json.gz || true',
        retries=3,
    )

    t2 = BashOperator(
        task_id='first_remove_reviews.json.gz_if_it_exists',
        depends_on_past=False,
        bash_command=f'rm {absolutePathToApp}/input_data/item_dedup.json.gz || true',
        retries=3,
    )


    t3 = PythonOperator(
        task_id='download_json_gzip_file_reviews',
        python_callable=download_file,
        provide_context=True,
        op_kwargs={
            "url": "https://s3-eu-west-1.amazonaws.com/bigdata-team/job-interview/item_dedup.json.gz",
            "path": f"{absolutePathToApp}/input_data/item_dedup.json.gz",
        },
        dag=dag)

    t4 = PythonOperator(
        task_id='download_json_gzip_file_metadata',
        python_callable=download_file,
        provide_context=True,
        op_kwargs={
            "url": "https://s3-eu-west-1.amazonaws.com/bigdata-team/job-interview/metadata.json.gz",
            "path": f"{absolutePathToApp}/input_data/metadata.json.gz",
        },
        dag=dag)


    t5 = BashOperator(
    task_id='split_reviews_json',
    depends_on_past=False,
    bash_command=f'gzcat {absolutePathToApp}/input_data/item_dedup.json.gz | split -l {splitSizeJsonGz} - "{absolutePathToApp}/input_data/reviews_split/splitReviews.json.gz-"',
    retries=2)


    t6 = BashOperator(
        task_id='remove_reviews.json.gz',
        depends_on_past=False,
        bash_command=f'rm {absolutePathToApp}/input_data/item_dedup.json.gz',
        retries=3,
    )

    t7 = BashOperator(
        task_id='split_metadata_json',
        depends_on_past=False,
        bash_command=f'gzcat {absolutePathToApp}/input_data/metadata.json.gz | split -l {splitSizeJsonGz} - "{absolutePathToApp}/input_data/metadata_split/splitMetadata.json.gz-"',
        retries=2
    )

    t8 = BashOperator(
        task_id='remove_metadata.json.gz',
        depends_on_past=False,
        bash_command=f'rm {absolutePathToApp}/input_data/metadata.json.gz',
        retries=3,
    )

    t9 = BashOperator(
        task_id='writing_dim_products_and_dim_bucket_price_to_disk',
        depends_on_past=False,
        bash_command=f"spark-submit {absolutePathToApp}/app_spark/process_metadata.py",
        retries=3,
    )

    t10 = BashOperator(
        task_id='writing_fact_reviews_and_dim_reviewers_to_disk',
        depends_on_past=False,
        bash_command=f"spark-submit {absolutePathToApp}/app_spark/process_reviews.py",
        retries=3,
    )

    t11 = BashOperator(
        task_id='info_delete_data_from_reviews_split',
        depends_on_past=False,
        bash_command=f'rm -f {absolutePathToApp}/input_data/reviews_split/s*',
        retries=3,
    )

    t12 = BashOperator(
        task_id='info_delete_data_from_metadata_split',
        depends_on_past=False,
        bash_command=f'rm -f {absolutePathToApp}/input_data/metadata_split/s*',
        retries=3,
    )

    t13 = BashOperator(
        task_id='create_database_and_tables',
        depends_on_past=False,
        bash_command=f"bash {absolutePathToApp}/scripts_psql/create_tables_psql.sh ",
        retries=3,
    )
    # From t13 to below it didn't work - got kind of disappointed by that - bash and airflow aren't best friends
    t14 = BashOperator(
        task_id='insert_in_fact_reviews',
        depends_on_past=False,
        bash_command=f"bash {absolutePathToApp}/scripts_psql/insert_into_fact_reviews.sh ",
        retries=3,
    )

    t15 = BashOperator(
        task_id='insert_dim_reviewers',
        depends_on_past=False,
        bash_command=f"bash {absolutePathToApp}/scripts_psql/insert_into_dim_reviewers.sh ",
        retries=3,
    )

    t16 = BashOperator(
        task_id='insert_in_dim_products',
        depends_on_past=False,
        bash_command=f"bash {absolutePathToApp}/scripts_psql/insert_into_dim_products.sh ",
        retries=3,
    )

    t17 = BashOperator(
        task_id='insert_dim_categories',
        depends_on_past=False,
        bash_command=f"bash {absolutePathToApp}/scripts_psql/insert_into_dim_categories.sh ",
        retries=3,
    )

    t18 = BashOperator(
        task_id='insert_dim_buckets',
        depends_on_past=False,
        bash_command=f"bash {absolutePathToApp}/scripts_psql/insert_into_dim_buckets.sh ",
        retries=3,
    )

    t19 = BashOperator(
        task_id='delete_output_files_from_disk',
        depends_on_past=False,
        bash_command=f"rm -rf {absolutePathToApp}/output_data/**",
        retries=3
    )

    t20 = PythonOperator(
        task_id='generate_viz_report_and_send_emails',
        python_callable=generate_viz_report,
        provide_context=True,
        op_kwargs={
            "email_list": ["receiveremail1@gmail.com"],
            "sender_email": "yoursenderemailhere@gmail.com",
            "sender_password": "putyourpasshere",
        },
        dag=dag)

    t2 >> t3 >> t4 >> t7 >> t8 >> t9 >> t12 >> t13 >> t16 >> t17 >> t18 >> t19 >> t20
    t1 >> t3 >> t4 >> t5 >> t6 >> t10 >> t11 >> t13 >> t14 >> t15 >> t19 >> t20
