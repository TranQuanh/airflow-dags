from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    '00_centralized_git_sync',
    start_date=datetime(2026, 5, 1),
    schedule_interval=None,
    catchup=False,
    tags=['infrastructure']
) as dag:

    # BƯỚC 1: MASTER ĐẨY CODE (Chạy trên Scheduler/Master)
    push_from_master = BashOperator(
        task_id='push_from_master',
        bash_command="""
        cd /opt/airflow/dags && \
        git config --global --add safe.directory /opt/airflow/dags && \
        git add . && \
        git commit -m "Code updated from Master at $(date)" || echo "No changes" && \
        git push origin main
        """,
        # Không cần để queue, mặc định Scheduler sẽ xử lý hoặc đẩy vào default queue
    )

    # BƯỚC 2: WORKER KÉO CODE (Chạy trên Worker CentOS)
    # Anh cần đảm bảo Worker CentOS của Anh đang lắng nghe queue này
    pull_to_worker = BashOperator(
        task_id='pull_to_worker_centos',
        bash_command="""
        cd /opt/airflow/dags && \
        git config --global --add safe.directory /opt/airflow/dags && \
        git fetch --all && \
        git reset --hard origin/main && \
        chown -R 50000:0 /opt/airflow/dags
        """,
        queue='worker_queue' # <--- ÉP lệnh này phải chạy trên con Worker CentOS
    )

    push_from_master >> pull_to_worker