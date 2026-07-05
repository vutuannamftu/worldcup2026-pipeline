import os

# API-Football
API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "")
API_FOOTBALL_HOST = "v3.football.api-sports.io"
API_FOOTBALL_BASE_URL = f"https://{API_FOOTBALL_HOST}"

# World Cup 2026
WC_LEAGUE_ID = 1
WC_SEASON = 2022  # Free tier chỉ hỗ trợ 2022-2024; đổi lại 2026 khi upgrade plan

# PostgreSQL project database
DB_CONFIG = {
    "host": os.getenv("PROJECT_DB_HOST", "postgres-project"),
    "port": int(os.getenv("PROJECT_DB_PORT", 5432)),
    "dbname": os.getenv("PROJECT_DB_NAME", "worldcup2026"),
    "user": os.getenv("PROJECT_DB_USER", "postgres"),
    "password": os.getenv("PROJECT_DB_PASSWORD", ""),
}

# Bronze layer: raw JSON lưu vào đây
BRONZE_DATA_DIR = os.getenv("BRONZE_DATA_DIR", "/opt/airflow/data/bronze")
