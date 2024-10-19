from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 14),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
        'youtube-data-ingestion-s3',
        default_args=default_args,
        schedule_interval=None,
    ) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=lambda: print("Jobs started"),
        dag=dag
    )
    
    youtube_data_ingestion_task = BashOperator(
        task_id='youtube_data_ingestion',
        bash_command='python /opt/airflow/jobs/youtube_data_ingestion.py',
    )
    
    end = PythonOperator(
        task_id="end",
        python_callable=lambda: print("Jobs completed successfully"),
        dag=dag
    )

    start >> youtube_data_ingestion_task >> end
