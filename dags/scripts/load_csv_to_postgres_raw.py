from __future__ import annotations

import os
import sys
import uuid
import pandas as pd
import psycopg2
# Thêm import Hook của Airflow
from airflow.providers.postgres.hooks.postgres import PostgresHook

RUN_ID = f"RUN_{uuid.uuid4().hex[:8]}"

# Lấy kết nối từ Airflow Connection ID thay vì biến env thủ công
try:
    pg_hook = PostgresHook(postgres_conn_id="final-project")
    connection_object = pg_hook.get_connection("final-project")
    
    DB = {
        "host": connection_object.host,
        "port": connection_object.port or 5432,
        "database": connection_object.schema,
        "user": connection_object.login,
        "password": connection_object.password,
    }
except Exception as hook_err:
    print(f"Không thể kết nối Airflow Connection ID 'final-project': {hook_err}")
    sys.exit(1)

# Vẫn giữ lại CSV_PATH từ env vì đây là đường dẫn file
# Sửa lại dòng này trong file load_csv_to_postgres_raw.py của bạn
CSV_PATH = os.getenv("CSV_PATH", "/opt/airflow/dags/data/Supermarket_sales..csv.csv")
SQL_LOG = "INSERT INTO etl.etl_log(run_id, step_name, status, message) VALUES (%s,%s,%s,%s);"

def main() -> None:
    conn = psycopg2.connect(**DB)
    conn.autocommit = False
    try:
        df = pd.read_csv(CSV_PATH, dtype=str).fillna("")
        
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
            cur.execute("CREATE SCHEMA IF NOT EXISTS etl;")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS etl.etl_log (
                    run_id TEXT, step_name TEXT, status TEXT, message TEXT, log_time TIMESTAMP DEFAULT NOW()
                );
            """)
            cur.execute(SQL_LOG, (RUN_ID, "INGEST", "START", f"Loading CSV: {CSV_PATH} rows={len(df)}"))
            cur.execute("""
                DROP TABLE IF EXISTS raw.sales_raw;
                CREATE TABLE raw.sales_raw (
                    raw_id SERIAL PRIMARY KEY,
                    invoice_id_text TEXT, branch_text TEXT, city_text TEXT, customer_type_text TEXT,
                    gender_text TEXT, product_line_text TEXT, unit_price_text TEXT, quantity_text TEXT,
                    tax_text TEXT, total_text TEXT, date_text TEXT, time_text TEXT,
                    payment_text TEXT, cogs_text TEXT, gross_margin_text TEXT, gross_income_text TEXT, rating_text TEXT
                );
            """)

            insert_sql = '''
                INSERT INTO raw.sales_raw(
                    invoice_id_text, branch_text, city_text, customer_type_text,
                    gender_text, product_line_text, unit_price_text, quantity_text,
                    tax_text, total_text, date_text, time_text,
                    payment_text, cogs_text, gross_margin_text, gross_income_text, rating_text
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            '''
            
            rows = df[[
                "Invoice ID", "Branch", "City", "Customer type", 
                "Gender", "Product line", "Unit price", "Quantity", 
                "Tax 5%", "Total", "Date", "Time", 
                "Payment", "cogs", "gross margin percentage", "gross income", "Rating"
            ]].values.tolist()
            
            cur.executemany(insert_sql, rows)
            cur.execute(SQL_LOG, (RUN_ID, "INGEST", "SUCCESS", f"Inserted rows={len(rows)}"))
            
        conn.commit()
        print(f"INGEST PASSED | RUN_ID={RUN_ID} | rows={len(df)}")
        return
    except Exception as e:
        conn.rollback()
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(SQL_LOG, (RUN_ID, "INGEST", "FAIL", str(e)))
        except Exception:
            pass
        print(f"INGEST FAILED | RUN_ID={RUN_ID} | ERROR={e}")
        raise e  
    finally:
        conn.close()

if __name__ == "__main__":
    main()