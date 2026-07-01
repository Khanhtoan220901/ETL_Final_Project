from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
# Import SmtpNotifier chuẩn theo Airflow 3 Provider
from airflow.providers.smtp.notifications.smtp import SmtpNotifier 

sys.path.append("/opt/airflow/dags/scripts")

import load_csv_to_postgres_raw
import etl_sales_transform_load
import post_run_check

DAG_ID = "sales_e2e_csv_to_dw"

# Email nhận thông báo lỗi
RECEIVER_EMAILS = ["khanhtoanlenguyenhn@gmail.com"]

# Định nghĩa Notifier sử dụng Connection mặc định
failure_email_notifier = SmtpNotifier(
    smtp_conn_id="smtp_default",       # Sử dụng Connection ID đã khai báo trong docker-compose
    from_email="khanhtoanlenguyen288@gmail.com", # Email gửi đi
    to=RECEIVER_EMAILS,                # Email nhận
    subject="[🔥 AIRFLOW ALERT] Task {{ ti.task_id }} đã bị lỗi!",
    html_content="""
    <h3>Thông báo lỗi luồng dữ liệu ETL (Qua Airflow Connection)</h3>
    <p><b>DAG ID:</b> {{ dag.dag_id }}</p>
    <p><b>Task ID:</b> {{ ti.task_id }}</p>
    <p><b>Thời gian chạy:</b> {{ dag_run.logical_date }}</p>
    <p><b>Trạng thái Task:</b> <span style="color:red; font-weight:bold;">FAILED</span></p>
    <p><b>Chi tiết lỗi (Exception):</b></p>
    <pre style="background-color: #f8f9fa; padding: 10px; border: 1px solid #dee2e6;">{{ exception }}</pre>
    <br>
    <p>👉 <i>Vui lòng kiểm tra Airflow UI để biết thêm chi tiết.</i></p>
    """
)

default_args = {
    "owner": "mindx_de", 
    "retries": 2, 
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": failure_email_notifier, # Tự động gọi mail khi có lỗi log bất kỳ task nào
}

def run_ingest(csv_path):
    os.environ["CSV_PATH"] = csv_path
    load_csv_to_postgres_raw.main()

with DAG(
    dag_id=DAG_ID,
    description="E2E demo: CSV -> Postgres(raw) -> DW using ETL + Airflow Connection",
    default_args=default_args,
    start_date=datetime(2026, 6, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "sales", "etl"],
) as dag:
    
    start = EmptyOperator(task_id="start")

    ingest = PythonOperator(
        task_id="ingest_csv_to_raw",
        python_callable=run_ingest,
        op_kwargs={
            "csv_path": "/opt/airflow/dags/data/Supermarket_sales.csv"
        },
    )

    transform_load = PythonOperator(
        task_id="transform_load_dw",
        python_callable=etl_sales_transform_load.main,
    )

    check = PythonOperator(
        task_id="post_run_check",
        python_callable=post_run_check.main,
    )

    end = EmptyOperator(task_id="end")

    start >> ingest >> transform_load >> check >> end