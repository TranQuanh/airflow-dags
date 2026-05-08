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
        cd /opt/airflow/dags || exit 1; \
        # Sử dụng cấu hình local thay vì global
        git config user.email "pewpewls09@example.com" && \
        git config user.name "Tran Quang Anh" && \
        git config --add safe.directory /opt/airflow/dags && \
        git add . && \
        # Nếu vẫn báo lỗi identity, ép thông tin trực tiếp vào lệnh commit
        git commit -m "Sync from Master at $(date)" --author="Tran Quang Anh <pewpewls09@example.com>" || echo "Nothing to commit"; \
        git push origin main
        """,
        queue='default'
    )

    # BƯỚC 2: CHẠY TRÊN CENTOS (Worker)
    # Task này sẽ vào queue 'worker_centos'
    pull_to_worker = BashOperator(
        task_id='pull_to_worker_centos',
        bash_command="""
        echo "Worker (CentOS) đang kéo code mới..." && sleep 10 && \
        cd /opt/airflow/dags || exit 1; \
        # Khai báo an toàn cho thư mục git
        git config --global --add safe.directory /opt/airflow/dags && \
        # Đồng bộ lại giờ nếu có thể (tránh lỗi SSL/Token khi fetch)
        # git fetch --all
        git fetch origin main && \
        # Ép code về giống hệt bản trên Git, xóa bỏ mọi thay đổi cục bộ
        git reset --hard origin/main && \
        # Quan trọng: Trả lại quyền sở hữu cho user airflow sau khi pull
        chown -R 50000:0 /opt/airflow/dags
        """,
        queue='worker_centos' # <--- Máy CentOS nhận
    )

    push_from_master >> pull_to_worker