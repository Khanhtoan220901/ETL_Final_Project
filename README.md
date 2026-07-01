 Sales Airflow E2E Project (CSV -> Postgres -> DW)

## Quick start
1) Open Docker Desktop (wait until Running)
2) Open and write .env : "AIRFLOW__CONN__SMTP_DEFAULT=smtp://name_email%40gmail.com:password@smtp.gmail.com:587?disable_tls=False&disable_ssl=True"
3) Open and change RECEIVER_EMAILS and from_email
2) Open CMD in this folder
3) First run (init + create admin user):
   docker compose up airflow-init
4) Start services:
   docker compose up -d
5) Open Airflow UI:
   http://localhost:8080
   admin / admin
6) Trigger DAG:
   sales_e2e_csv_to_dw

## Verify (optional)
Connection Airflow 3 
- Connection ID : final-project
- Connection Type: Postgres
- Host : check in Docker ( ex : 173.32.0.2)
- user: airflow
- pass: airflow
- port : 5432
- db: final_project

Connection Airflow 3 SMTP
- Connection ID : smtp-default
- Connection Type: SMTP
- Host : smtp.gmail.com
- user: your_email
- pass: Use App Password 
- port : 587
- Extra fields Json : 
{ "disable_tls": false,
  "disable_ssl": true
}
## Reset
docker compose down -v
