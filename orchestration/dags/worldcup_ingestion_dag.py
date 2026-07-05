from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "vutuannam",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="worldcup_bronze_ingestion",
    description="Daily ingestion of World Cup 2026 data from API-Football into Bronze layer",
    schedule_interval="0 8 * * *",  # 8:00 AM UTC mỗi ngày (~3 PM Vietnam time)
    start_date=datetime(2022, 11, 20),
    catchup=False,
    default_args=default_args,
    tags=["worldcup", "ingestion", "bronze"],
) as dag:

    def init_tables():
        import sys
        sys.path.insert(0, "/opt/airflow")
        from ingestion.bronze import init_bronze_tables
        init_bronze_tables()

    def ingest_fixtures():
        import sys
        sys.path.insert(0, "/opt/airflow")
        from ingestion.extractor import run_fixtures
        run_fixtures()

    def ingest_match_details():
        import sys
        sys.path.insert(0, "/opt/airflow")
        from ingestion.extractor import run_match_details
        count = run_match_details()
        print(f"Processed {count} new fixtures this run")

    task_init = PythonOperator(
        task_id="init_bronze_tables",
        python_callable=init_tables,
    )

    task_fixtures = PythonOperator(
        task_id="ingest_fixtures",
        python_callable=ingest_fixtures,
    )

    task_details = PythonOperator(
        task_id="ingest_match_details",
        python_callable=ingest_match_details,
    )

    task_init >> task_fixtures >> task_details
