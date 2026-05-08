from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import socket
import os
# tesst
def check_worker_environment():
    print("="*30)
    print("KẾT QUẢ KIỂM TRA ĐỒNG BỘ")
    print(f"Hostname hiện tại: {socket.gethostname()}")
    print(f"Thư mục đang chạy: {os.getcwd()}")
    print(f"Thời gian hệ thống: {datetime.now()}")
    print("Trạng thái: ĐỒNG BỘ GIT-SYNC THÀNH CÔNG!")
    print("="*30)

default_args = {
    'owner': 'quanganh',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 7),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'test_final_sync_logic',
    default_args=default_args,
    description='DAG cuối cùng để kiểm tra đồng bộ giữa Master và Worker',
    schedule_interval=None,
    catchup=False,
    tags=['production', 'test_sync']
) as dag:

    task_check = PythonOperator(
        task_id='verify_environment_info',
        python_callable=check_worker_environment,
    )