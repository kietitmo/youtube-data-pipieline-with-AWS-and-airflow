from airflow import DAG
from airflow.operators.python_operator  import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator  import BashOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models.param import Param

from datetime import datetime, timedelta
from airflow.utils.session import create_session
from airflow.models import Connection

import os
import logging
import time

AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')
DOWNLOAD_PATH = os.getenv('DOWNLOAD_PATH')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 14),
    'retries': 3,  # Số lần thử lại
    'retry_delay': timedelta(minutes=5), 
}


with DAG(
        'youtube-data-ingestion-s3',
        default_args=default_args,
        schedule_interval=None,
        params={
            "dataset_version": Param(1246, type="integer", minimum=3),
        },
    ) as dag:

    version = dag.params['dataset_version']
    username = 'rsrishav'
    dataset = 'youtube-trending-video-dataset'

    # Tải dataset từ kaggle
    download_dataset_task = BashOperator(
        task_id='download_dataset_task',
        bash_command=f'kaggle datasets download {username}/{dataset}/{version} -p {DOWNLOAD_PATH} --unzip'
    )
 
    def check_aws_connection():
        with create_session() as session:
            connection = session.query(Connection).filter_by(conn_id='aws_default').first()
            return 'create_aws_connection_task' if connection is None else 'get_csv_files_task'
    
    # Task kiểm tra kết nối AWS
    check_connection_task = BranchPythonOperator(
        task_id='check_aws_connection_task',
        python_callable=check_aws_connection,
    )

    def create_aws_connection():
        conn = Connection(
            conn_id='aws_default', 
            conn_type='aws',
            login=AWS_ACCESS_KEY,
            password=AWS_SECRET_KEY,
        )

        with create_session() as session:
            session.add(conn)
            session.commit()

    create_connection_task = PythonOperator(
        task_id='create_aws_connection_task',
        python_callable=create_aws_connection,
    )

    def get_csv_files_to_upload():
        files = []
        for filename in os.listdir(DOWNLOAD_PATH):
            if filename.endswith('.csv'):
                files.append(os.path.join(DOWNLOAD_PATH, filename))
        return files

    # Task để lấy danh sách file CSV cần upload
    get_csv_files_task = PythonOperator(
        task_id='get_csv_files_task',
        python_callable=get_csv_files_to_upload,
        do_xcom_push=True,
    )

    # Lấy danh sách file JSON cần upload
    def get_json_files_to_upload():
        files = []
        for filename in os.listdir(DOWNLOAD_PATH):
            if filename.endswith('.json'):
                files.append(os.path.join(DOWNLOAD_PATH, filename))
        return files

    # Task để lấy danh sách file JSON cần upload
    get_json_files_task = PythonOperator(
        task_id='get_json_files_task',
        python_callable=get_json_files_to_upload,
        do_xcom_push=True,
    )

    def upload_csv_files_to_s3(**kwargs):
        s3_hook = S3Hook(aws_conn_id='aws_default')
        csv_files = kwargs['ti'].xcom_pull(task_ids='get_csv_files_task')
        
        for file_path in csv_files:
            filename = os.path.basename(file_path)
            region = filename.split('_')[0]  # Giả sử region được lấy từ tiền tố của tên file
            upload_date = datetime.now().strftime('%Y-%m-%d')  # Lấy ngày hiện tại

            key_prefix = f'youtube_trending_csv/upload_date={upload_date}/region={region}/'
            retries = 3  # Số lần thử lại
            
            for attempt in range(retries):
                try:
                    s3_hook.load_file(
                       filename=file_path,
                        key=f'{key_prefix}{filename}',
                        bucket_name=BUCKET_NAME,
                        replace=True,
                    )
                    logging.info(f'Successfully uploaded {file_path} to S3.')
                    break  # Thoát khỏi vòng lặp nếu tải lên thành công
                except Exception as e:
                    logging.error(f'Attempt {attempt + 1} failed to upload {file_path} to S3: {e}')
                    time.sleep(2)  # Chờ 2 giây trước khi thử lại
                    if attempt == retries - 1:
                        logging.critical(f'Failed to upload {file_path} after {retries} attempts.')

    def upload_json_files_to_s3(**kwargs):
        s3_hook = S3Hook(aws_conn_id='aws_default')
        json_files = kwargs['ti'].xcom_pull(task_ids='get_json_files_task')
        
        for file_path in json_files:
            filename = os.path.basename(file_path)
            region = filename.split('_')[0]  # Giả sử region được lấy từ tiền tố của tên file
            upload_date = datetime.now().strftime('%Y-%m-%d')  # Lấy ngày hiện tại

            key_prefix = f'youtube_category_json/upload_date={upload_date}/region={region}/'
            retries = 3  # Số lần thử lại
            for attempt in range(retries):
                try:
                    s3_hook.load_file(
                        filename=file_path,
                        key=f'{key_prefix}{filename}',
                        bucket_name=BUCKET_NAME,
                        replace=True,
                    )
                    logging.info(f'Successfully uploaded {file_path} to S3.')
                    break  # Thoát khỏi vòng lặp nếu tải lên thành công
                except Exception as e:
                    logging.error(f'Attempt {attempt + 1} failed to upload {file_path} to S3: {e}')
                    time.sleep(2)  # Chờ 2 giây trước khi thử lại
                    if attempt == retries - 1:
                        logging.critical(f'Failed to upload {file_path} after {retries} attempts.')

    upload_csv_trending_task = PythonOperator(
        task_id='upload_csv_files_to_s3_task',
        python_callable=upload_csv_files_to_s3,
    )

    upload_json_category_task = PythonOperator(
        task_id='upload_json_files_to_s3_task',
        python_callable=upload_json_files_to_s3,
    )

    def delete_downloaded_files():
        for filename in os.listdir(DOWNLOAD_PATH):
            file_path = os.path.join(DOWNLOAD_PATH, filename)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    logging.info(f'Successfully deleted {file_path}.')
                else:
                    logging.warning(f'File not found: {file_path}.')
            except Exception as e:
                logging.error(f'Error deleting file {file_path}: {e}')

    delete_files_task = PythonOperator(
        task_id='delete_downloaded_files_task',
        python_callable=delete_downloaded_files,
    )

    download_dataset_task >> check_connection_task 

    check_connection_task  >> create_connection_task >> get_csv_files_task
    check_connection_task >> create_connection_task >> get_json_files_task

    get_csv_files_task >> upload_csv_trending_task >> delete_files_task
    get_json_files_task >> upload_json_category_task >> delete_files_task
