from __future__ import annotations

import sys
import psycopg2
from airflow.providers.postgres.hooks.postgres import PostgresHook

def main():
    # Lấy thông tin kết nối từ Airflow Connection 'final-project'
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
        return

    conn = psycopg2.connect(**DB)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw.sales_raw;")
            raw_cnt = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM dw.sales_fact;")
            dw_cnt = cur.fetchone()[0]
            
            # Tính tổng doanh thu theo cột total của tập dữ liệu mới
            cur.execute("SELECT COALESCE(SUM(total), 0) FROM dw.sales_fact;")
            rev = cur.fetchone()[0]
            
        print(f"CHECK OK | raw_cnt={raw_cnt} | dw_cnt={dw_cnt} | total_revenue={rev}")
    except Exception as e:
        print(f"CHECK FAILED | ERROR={e}")
        raise e
    finally:
        conn.close()

if __name__ == "__main__":
    main()