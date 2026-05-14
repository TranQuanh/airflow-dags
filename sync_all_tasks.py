from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# 1. CẤU HÌNH HỆ THỐNG
GIT_USER_EMAIL = "pewpewls09@example.com"
GIT_USER_NAME = "Tran Quang Anh"

WORKERS_LIST = [
    {"id": "centos_01", "queue": "worker_centos"}
]

with DAG(
    '01_sync_infra_v25_with_detailed_logs', 
    start_date=datetime(2026, 5, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=['infrastructure']
) as dag:

    # ---------------------------------------------------------
    # PHASE 1: PRIMARY PUSH (UBUNTU CHÍNH)
    # ---------------------------------------------------------
    sync_primary = BashOperator(
        task_id='sync_primary_ubuntu',
        bash_command=f"""
        cd /opt/airflow/dags || exit 1;
        git config user.email "{GIT_USER_EMAIL}"
        git config user.name "{GIT_USER_NAME}"
        git config --add safe.directory /opt/airflow/dags
        git fetch --all
        
        echo "[LOG] Bắt đầu kiểm tra Primary Ubuntu..."
        
        if git branch -r | grep -q "origin/conflict-"; then
            echo "[ERROR] DỪNG: Phát hiện nhánh conflict trên GitHub. Admin cần xử lý trước!"; exit 1
        fi

        LOCAL=$(git rev-parse @); REMOTE=$(git rev-parse @{{u}}); HAS_CHANGES=$(git status -s)

        if [ "$LOCAL" != "$REMOTE" ]; then
            if [ -z "$HAS_CHANGES" ]; then
                echo "[CASE 1] Ubuntu sạch nhưng cũ. Hành động: Hard Reset về origin/main."
                git reset --hard origin/main; exit 0
            else
                echo "[ERROR] Ubuntu có code dở và GitHub đã thay đổi. Dừng để tránh mất code!"; exit 1
            fi
        fi

        if [ -n "$HAS_CHANGES" ]; then
            echo "[CASE 2] Ubuntu có thay đổi mới. Hành động: Commit và Push lên GitHub."
            git add .; git commit -m "Primary update: $(date)"; git push origin main
        else
            echo "[CASE 3] Ubuntu đã đồng bộ hoàn toàn với GitHub. Không cần làm gì."
        fi
        """,
        queue='default'
    )

    # ---------------------------------------------------------
    # PHASE 2: WORKER PUSH CHAIN (LẦN LƯỢT TỪNG MÁY PHỤ)
    # ---------------------------------------------------------
    previous_task = sync_primary

    for worker in WORKERS_LIST:
        w_id = worker["id"]
        
        sync_worker_task = BashOperator(
            task_id=f'push_sync_{w_id}',
            bash_command=f"""
            cd /opt/airflow/dags || exit 1;
            rm -f .git/index.lock || true
            git config user.email "{GIT_USER_EMAIL}"
            git config user.name "{GIT_USER_NAME}"
            git config --add safe.directory /opt/airflow/dags
            git fetch --all

            echo "[LOG] Bắt đầu kiểm tra máy {w_id}..."

            LOCAL=$(git rev-parse @); REMOTE=$(git rev-parse @{{u}}); HAS_CHANGES=$(git status -s)

            if [ "$LOCAL" != "$REMOTE" ] && [ -z "$HAS_CHANGES" ]; then
                echo "[CASE 1] {w_id} sạch nhưng cũ. Hành động: Hard Reset về bản GitHub mới nhất."
                git reset --hard origin/main; exit 0
            fi

            if [ -n "$HAS_CHANGES" ]; then
                echo "[LOG] {w_id} phát hiện có thay đổi local. Đang tạo commit..."
                git add .; git commit -m "{w_id} update: $(date)"
                
                REMOTE_NEW=$(git rev-parse @{{u}})
                if [ "$(git merge-base @ @{{u}})" == "$REMOTE_NEW" ]; then
                    echo "[CASE 2] GitHub trống. Hành động: Push thẳng từ {w_id}."
                    git push origin main
                else
                    echo "[CASE 3] GitHub đã có code mới từ máy khác. Hành động: Pull & Merge."
                    if git pull origin main --no-rebase; then
                        echo "[LOG] Merge thành công. Đang đẩy code tổng hợp lên GitHub..."
                        git push origin main
                    else
                        echo "[ERROR] PHÁT HIỆN CONFLICT tại {w_id}! Đang hủy merge và đẩy nhánh lỗi..."
                        git merge --abort
                        BRANCH_NAME="conflict-{w_id}-$(date +%m%d-%H%M)"
                        git checkout -b $BRANCH_NAME; git push origin $BRANCH_NAME; git checkout main; exit 1
                    fi
                fi
            else
                echo "[CASE 4] {w_id} không có thay đổi nào để đẩy."
            fi
            """,
            queue=worker["queue"]
        )
        
        previous_task >> sync_worker_task
        previous_task = sync_worker_task

    # ---------------------------------------------------------
    # PHASE 3: GLOBAL FINAL RESET (ĐỒNG BỘ TUẦN TỰ CUỐI CÙNG)
    # ---------------------------------------------------------
    final_reset_primary = BashOperator(
        task_id='final_reset_primary_ubuntu',
        bash_command="""
        cd /opt/airflow/dags || exit 1;
        git fetch --all
        echo "[LOG] Kiểm tra đồng bộ cuối cho Primary Ubuntu..."
        if [ -z "$(git status -s)" ]; then
            echo "[ACTION] Máy sạch. Thực hiện Reset về bản mới nhất vừa tổng hợp."
            git reset --hard origin/main
        else
            echo "[WARNING] Máy không sạch, bỏ qua Reset cuối để bảo vệ code."
        fi
        """,
        queue='default'
    )
    
    previous_task >> final_reset_primary
    last_reset_task = final_reset_primary

    for worker in WORKERS_LIST:
        reset_worker = BashOperator(
            task_id=f'final_reset_{worker["id"]}',
            bash_command=f"""
            cd /opt/airflow/dags || exit 1;
            git fetch --all
            echo "[LOG] Kiểm tra đồng bộ cuối cho máy {worker["id"]}..."
            if [ -z "$(git status -s)" ]; then
                echo "[ACTION] {worker["id"]} sạch. Thực hiện Reset về bản mới nhất từ GitHub."
                git reset --hard origin/main
            else
                echo "[ERROR] {worker["id"]} đang có code dở. Dừng chuỗi đồng bộ!"; exit 1
            fi
            """,
            queue=worker["queue"]
        )
        last_reset_task >> reset_worker
        last_reset_task = reset_worker