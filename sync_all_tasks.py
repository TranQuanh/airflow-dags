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
    '01_sync_infra_v23_global_reset', 
    start_date=datetime(2026, 5, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=['infrastructure']
) as dag:

    # ---------------------------------------------------------
    # PHASE 1: PRIMARY SYNC (ĐẨY CODE ĐẦU TIÊN TỪ MÁY CHỦ)
    # ---------------------------------------------------------
    sync_primary = BashOperator(
        task_id='sync_primary_ubuntu',
        bash_command=f"""
        cd /opt/airflow/dags || exit 1;
        git config user.email "{GIT_USER_EMAIL}"
        git config user.name "{GIT_USER_NAME}"
        git config --add safe.directory /opt/airflow/dags
        git fetch --all
        
        if git branch -r | grep -q "origin/conflict-"; then
            echo "DỪNG: Có conflict chưa xử lý trên GitHub!"; exit 1
        fi

        LOCAL=$(git rev-parse @); REMOTE=$(git rev-parse @{{u}}); HAS_CHANGES=$(git status -s)

        if [ "$LOCAL" != "$REMOTE" ] && [ -z "$HAS_CHANGES" ]; then
            git reset --hard origin/main; exit 0
        fi

        if [ -n "$HAS_CHANGES" ]; then
            git add .; git commit -m "Primary update: $(date)"; git push origin main
        fi
        """,
        queue='default'
    )

    # ---------------------------------------------------------
    # PHASE 2: WORKER PUSH LOOP (CẬP NHẬT VÀ ĐẨY CODE TỪNG MÁY)
    # ---------------------------------------------------------
    previous_task = sync_primary

    for worker in WORKERS_LIST:
        worker_id = worker["id"]
        
        sync_worker_task = BashOperator(
            task_id=f'push_sync_{worker_id}',
            bash_command=f"""
            cd /opt/airflow/dags || exit 1;
            rm -f .git/index.lock || true
            git config user.email "{GIT_USER_EMAIL}"
            git config user.name "{GIT_USER_NAME}"
            git config --add safe.directory /opt/airflow/dags
            git fetch --all

            LOCAL=$(git rev-parse @); REMOTE=$(git rev-parse @{{u}}); HAS_CHANGES=$(git status -s)

            if [ "$LOCAL" != "$REMOTE" ] && [ -z "$HAS_CHANGES" ]; then
                git reset --hard origin/main; exit 0
            fi

            if [ -n "$HAS_CHANGES" ]; then
                git add .; git commit -m "{worker_id} update: $(date)"
                REMOTE_NEW=$(git rev-parse @{{u}})
                if [ "$(git merge-base @ @{{u}})" == "$REMOTE_NEW" ]; then
                    git push origin main
                else
                    if git pull origin main --no-rebase; then
                        git push origin main
                    else
                        git merge --abort
                        BRANCH_NAME="conflict-{worker_id}-$(date +%m%d-%H%M)"
                        git checkout -b $BRANCH_NAME; git push origin $BRANCH_NAME; git checkout main; exit 1
                    fi
                fi
            fi
            """,
            queue=worker["queue"]
        )
        
        previous_task >> sync_worker_task
        previous_task = sync_worker_task

    # ---------------------------------------------------------
    # PHASE 3: GLOBAL FINAL RESET (ĐỒNG BỘ TẤT CẢ VỀ BẢN MỚI NHẤT)
    # ---------------------------------------------------------
    final_reset_primary = BashOperator(
        task_id='final_reset_primary_ubuntu',
        bash_command="""
        cd /opt/airflow/dags || exit 1;
        git fetch --all
        [ -z "$(git status -s)" ] && git reset --hard origin/main
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
            if [ -z "$(git status -s)" ]; then
                git reset --hard origin/main
                echo "{worker["id"]} đã đồng bộ thành công."
            else
                echo "LỖI: {worker["id"]} có code dở, không thể reset tuần tự!"; exit 1
            fi
            """,
            queue=worker["queue"]
        )
        last_reset_task >> reset_worker
        last_reset_task = reset_worker