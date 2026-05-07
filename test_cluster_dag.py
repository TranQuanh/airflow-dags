from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import socket

def print_info():
    hostname = socket.gethostname()
    print(f"--- THÔNG TIN THỰC THI ---")
    print(f"Task đang chạy trên Hostname: {hostname}")
    print(f"Thời gian hiện tại: {datetime.now()}")
    print(f"--------------------------")

default_args = {
    'owner': 'quanganh',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'check_worker_connection',
    default_args=default_args,
    description='DAG kiểm tra kết nối giữa Master và Worker',
    schedule_interval=None, # Chạy thủ công để test
    catchup=False,
    tags=['test', 'infrastructure'],
) as dag:

    run_test = PythonOperator(
        task_id='print_execution_host',
        python_callable=print_info,
    )