from __future__ import annotations

import os
import sys
import uuid
import psycopg2
from airflow.providers.postgres.hooks.postgres import PostgresHook

RUN_ID = f"RUN_{uuid.uuid4().hex[:8]}"

# Lấy cấu hình kết nối từ Airflow
try:
    pg_hook = PostgresHook(postgres_conn_id="final-project")
    connection_object = pg_hook.get_connection("final-project")
    
    DB = {
        "host": connection_object.host,
        "port": connection_object.port or 5432,
        "database": connection_object.schema,
        "user": connection_object.login,
        "password": connection_object.password,
        "connect_timeout": 10,
    }
except Exception as hook_err:
    print(f"Không thể kết nối Airflow Connection ID 'final-project': {hook_err}")
    sys.exit(1)

SQL_LOG = "INSERT INTO etl.etl_log(run_id, step_name, status, message) VALUES (%s,%s,%s,%s);"

SQL_BUILD_STAGING = r'''
CREATE SCHEMA IF NOT EXISTS dw;
DROP TABLE IF EXISTS etl.sales_staging_clean;

CREATE TABLE etl.sales_staging_clean (
    invoice_id VARCHAR(50) PRIMARY KEY,
    branch CHAR(1),
    city VARCHAR(100),
    customer_type VARCHAR(50),
    gender VARCHAR(10),
    product_line VARCHAR(100),
    unit_price NUMERIC(10, 2) NOT NULL CHECK (unit_price >= 0),
    quantity INT NOT NULL CHECK (quantity > 0),
    tax_5_percent NUMERIC(10, 4),
    total NUMERIC(10, 4) NOT NULL CHECK (total >= 0),
    date DATE NOT NULL,
    time TIME,
    payment VARCHAR(50) CHECK (payment IN ('Cash', 'Credit card', 'Ewallet')),
    cogs NUMERIC(10, 2),
    gross_margin_percentage NUMERIC(10, 6),
    gross_income NUMERIC(10, 4),
    rating NUMERIC(3, 1)
);

WITH parsed AS (
    SELECT
        raw_id,
        NULLIF(invoice_id_text, '') AS invoice_id,
        UPPER(TRIM(branch_text)) AS branch,
        NULLIF(city_text, '') AS city,
        NULLIF(customer_type_text, '') AS customer_type,
        NULLIF(gender_text, '') AS gender,
        NULLIF(product_line_text, '') AS product_line,
        CASE WHEN unit_price_text ~ '^[0-9]*\.?[0-9]+$' THEN unit_price_text::NUMERIC(10,2) ELSE NULL END AS unit_price,
        CASE WHEN quantity_text ~ '^[0-9]+$' THEN quantity_text::INT ELSE NULL END AS quantity,
        CASE WHEN tax_text ~ '^[0-9]*\.?[0-9]+$' THEN tax_text::NUMERIC(10,4) ELSE NULL END AS tax_5_percent,
        CASE WHEN total_text ~ '^[0-9]*\.?[0-9]+$' THEN total_text::NUMERIC(10,4) ELSE NULL END AS total,
        CASE 
            WHEN date_text ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN to_date(date_text, 'MM/DD/YYYY')
            WHEN date_text ~ '^\d{4}-\d{2}-\d{2}$' THEN date_text::DATE
            ELSE NULL 
        END AS date,
        CASE WHEN time_text ~ '^\d{2}:\d{2}$' THEN time_text::TIME ELSE NULL END AS time,
        TRIM(payment_text) AS payment,
        CASE WHEN cogs_text ~ '^[0-9]*\.?[0-9]+$' THEN cogs_text::NUMERIC(10,2) ELSE NULL END AS cogs,
        CASE WHEN gross_margin_text ~ '^[0-9]*\.?[0-9]+$' THEN gross_margin_text::NUMERIC(10,6) ELSE NULL END AS gross_margin_percentage,
        CASE WHEN gross_income_text ~ '^[0-9]*\.?[0-9]+$' THEN gross_income_text::NUMERIC(10,4) ELSE NULL END AS gross_income,
        CASE WHEN rating_text ~ '^[0-9]*\.?[0-9]+$' THEN rating_text::NUMERIC(3,1) ELSE NULL END AS rating
    FROM raw.sales_raw
),
dedup AS (
    SELECT DISTINCT ON (invoice_id) *
    FROM parsed
    WHERE invoice_id IS NOT NULL
    ORDER BY invoice_id, raw_id DESC
),
cleaned AS (
    SELECT * FROM dedup
    WHERE date IS NOT NULL
      AND quantity > 0
      AND unit_price >= 0
      AND total >= 0
      AND payment IN ('Cash', 'Credit card', 'Ewallet')
)
INSERT INTO etl.sales_staging_clean
SELECT invoice_id, branch, city, customer_type, gender, product_line, unit_price, quantity, 
       tax_5_percent, total, date, time, payment, cogs, gross_margin_percentage, gross_income, rating
FROM cleaned;
'''

SQL_UPSERT_DW = '''
CREATE TABLE IF NOT EXISTS dw.sales_fact (
    invoice_id VARCHAR(50) PRIMARY KEY,
    branch CHAR(1),
    city VARCHAR(100),
    customer_type VARCHAR(50),
    gender VARCHAR(10),
    product_line VARCHAR(100),
    unit_price NUMERIC(10, 2),
    quantity INT,
    tax_5_percent NUMERIC(10, 4),
    total NUMERIC(10, 4),
    date DATE,
    time TIME,
    payment VARCHAR(50),
    cogs NUMERIC(10, 2),
    gross_margin_percentage NUMERIC(10, 6),
    gross_income NUMERIC(10, 4),
    rating NUMERIC(3, 1),
    loaded_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO dw.sales_fact (
    invoice_id, branch, city, customer_type, gender, product_line, unit_price, quantity,
    tax_5_percent, total, date, time, payment, cogs, gross_margin_percentage, gross_income, rating, loaded_at
)
SELECT *, NOW() FROM etl.sales_staging_clean
ON CONFLICT (invoice_id) DO UPDATE SET
    branch = EXCLUDED.branch,
    city = EXCLUDED.city,
    customer_type = EXCLUDED.customer_type,
    product_line = EXCLUDED.product_line,
    unit_price = EXCLUDED.unit_price,
    quantity = EXCLUDED.quantity,
    total = EXCLUDED.total,
    payment = EXCLUDED.payment,
    rating = EXCLUDED.rating,
    loaded_at = NOW();
'''

SQL_VALIDATE_DUP = "SELECT invoice_id, COUNT(*) FROM dw.sales_fact GROUP BY invoice_id HAVING COUNT(*)>1;"
SQL_COUNT = "SELECT (SELECT COUNT(*) FROM etl.sales_staging_clean), (SELECT COUNT(*) FROM dw.sales_fact);"

def log(cur, step: str, status: str, msg: str = ""):
    cur.execute(SQL_LOG, (RUN_ID, step, status, msg))

def main():
    conn = psycopg2.connect(**DB)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            log(cur, "PIPELINE", "START", "Transform + Load sales")
            log(cur, "TRANSFORM", "START", "Build staging")
            cur.execute(SQL_BUILD_STAGING)
            log(cur, "TRANSFORM", "SUCCESS", "Staging built")

            log(cur, "LOAD", "START", "Upsert to DW")
            cur.execute(SQL_UPSERT_DW)
            log(cur, "LOAD", "SUCCESS", "Upsert done")

            log(cur, "VALIDATE", "START", "Check duplicates + counts")
            cur.execute(SQL_VALIDATE_DUP)
            if cur.fetchall():
                raise RuntimeError("Duplicate invoice_id detected in dw.sales_fact")

            cur.execute(SQL_COUNT)
            staging_cnt, dw_cnt = cur.fetchone()
            if staging_cnt <= 0:
                raise RuntimeError("staging_cnt=0 (no valid rows after cleaning)")
            log(cur, "VALIDATE", "SUCCESS", f"staging_cnt={staging_cnt}, dw_cnt={dw_cnt}")

            log(cur, "PIPELINE", "SUCCESS", "ETL finished")
        conn.commit()
        print(f"ETL PASSED | RUN_ID={RUN_ID}")
        return
    except Exception as e:
        conn.rollback()
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                log(cur, "PIPELINE", "FAIL", str(e))
        except Exception:
            pass
        print(f"ETL FAILED | RUN_ID={RUN_ID} | ERROR={e}")
        raise e
    finally:
        conn.close()

if __name__ == "__main__":
    main()