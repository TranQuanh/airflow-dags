from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    '01_sync_infra_v4', 
    start_date=datetime(2026, 5, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=['infrastructure']
) as dag:

    # BƯỚC 1: CHẠY TRÊN UBUNTU (Master)
    # Task này sẽ vào queue 'default' để con Worker trên Ubuntu xử lý
    push_from_master = BashOperator(
        task_id='push_from_master',
        bash_command="""
        echo "Master (Ubuntu) đang chuẩn bị đẩy code..." && sleep 10 && \
        cd /opt/airflow/dags && \
        git config --global --add safe.directory /opt/airflow/dags && \
        git add . && \
        git commit -m "Sync from Master at $(date)" || echo "Nothing to commit" && \
        git push origin main
        """,
        queue='default' # <--- Máy Ubuntu nhận
    )

    # BƯỚC 2: CHẠY TRÊN CENTOS (Worker)
    # Task này sẽ vào queue 'worker_centos'
    pull_to_worker = BashOperator(
        task_id='pull_to_worker_centos',
        bash_command="""
        echo "Worker (CentOS) đang kéo code mới..." && sleep 10 && \
        cd /opt/airflow/dags && \
        git config --global --add safe.directory /opt/airflow/dags && \
        git fetch --all && \
        git reset --hard origin/main && \
        chown -R 50000:0 /opt/airflow/dags
        """,
        queue='worker_centos' # <--- Máy CentOS nhận
    )

    push_from_master >> pull_to_worker