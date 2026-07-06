from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "vutuannam",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

with DAG(
    dag_id="worldcup_silver_transform",
    description="Transform Bronze raw JSON → Silver clean relational tables",
    schedule_interval="30 8 * * *",  # 8:30 AM UTC, 30 phút sau Bronze ingestion
    start_date=datetime(2022, 11, 20),
    catchup=False,
    default_args=default_args,
    tags=["worldcup", "transform", "silver"],
) as dag:

    def init_tables():
        import sys
        sys.path.insert(0, "/opt/airflow")
        from ingestion.silver import init_silver_tables
        init_silver_tables()

    def run_fixtures():
        import sys
        sys.path.insert(0, "/opt/airflow")
        from ingestion.silver import transform_fixtures
        count = transform_fixtures()
        print(f"Transformed {count} fixtures into silver_fixtures + silver_teams")

    def run_lineups():
        import sys
        sys.path.insert(0, "/opt/airflow")
        from ingestion.silver import transform_lineups
        count = transform_lineups()
        print(f"Transformed {count} lineup entries into silver_lineups + silver_lineup_players")

    def run_team_stats():
        import sys
        sys.path.insert(0, "/opt/airflow")
        from ingestion.silver import transform_team_stats
        count = transform_team_stats()
        print(f"Transformed {count} team stat rows into silver_team_stats")

    def run_player_stats():
        import sys
        sys.path.insert(0, "/opt/airflow")
        from ingestion.silver import transform_player_stats
        count = transform_player_stats()
        print(f"Transformed {count} player stat rows into silver_player_stats")

    task_init = PythonOperator(
        task_id="init_silver_tables",
        python_callable=init_tables,
    )

    task_fixtures = PythonOperator(
        task_id="transform_fixtures",
        python_callable=run_fixtures,
    )

    task_lineups = PythonOperator(
        task_id="transform_lineups",
        python_callable=run_lineups,
    )

    task_team_stats = PythonOperator(
        task_id="transform_team_stats",
        python_callable=run_team_stats,
    )

    task_player_stats = PythonOperator(
        task_id="transform_player_stats",
        python_callable=run_player_stats,
    )

    # fixtures + lineups có thể chạy song song sau init
    # team_stats + player_stats chạy song song sau đó
    task_init >> [task_fixtures, task_lineups]
    task_fixtures >> task_team_stats
    task_lineups >> task_player_stats
