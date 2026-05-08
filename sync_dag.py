from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    '01_sync_infra_v2', # Đổi tên để tránh bị lock bản ghi cũ
    start_date=datetime(2026, 5, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,    # Chỉ cho phép 1 lượt chạy tại 1 thời điểm
    tags=['infrastructure']
) as dag:

    # BƯỚC 1: MASTER ĐẨY CODE
    push_from_master = BashOperator(
        task_id='push_from_master',
        bash_command="""
        echo "Dang chuẩn bị đẩy code từ Master..." && sleep 5 && \
        cd /opt/airflow/dags || exit 1; \
        git config --global --add safe.directory /opt/airflow/dags && \
        git add . && \
        git commit -m "Auto-sync from Master at $(date)" || echo "No changes to commit"; \
        git push origin main && \
        echo "Master đã đẩy code thành công."
        """,
        queue='worker_centos' # Đảm bảo Worker CentOS đang lắng nghe queue này
    )

    # BƯỚC 2: WORKER KÉO CODE
    pull_to_worker = BashOperator(
        task_id='pull_to_worker_centos',
        bash_command="""
        echo "Dang chuẩn bị kéo code về Worker..." && sleep 5 && \
        cd /opt/airflow/dags || exit 1; \
        git config --global --add safe.directory /opt/airflow/dags && \
        git fetch --all && \
        git reset --hard origin/main && \
        chown -R 50000:0 /opt/airflow/dags && \
        echo "Worker đã đồng bộ và phân quyền thành công."
        """,
        queue='worker_centos' # Phải khớp với queue mà con CentOS đang chạy
    )

    push_from_master >> pull_to_worker