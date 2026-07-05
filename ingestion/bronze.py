import json
import os
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import Json

from ingestion.config import BRONZE_DATA_DIR, DB_CONFIG


def _get_conn():
    return psycopg2.connect(**DB_CONFIG)


def init_bronze_tables():
    ddl = """
    CREATE TABLE IF NOT EXISTS bronze_fixtures (
        id              SERIAL PRIMARY KEY,
        fixture_id      INTEGER NOT NULL,
        ingested_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        raw_json        JSONB NOT NULL
    );

    CREATE TABLE IF NOT EXISTS bronze_lineups (
        id              SERIAL PRIMARY KEY,
        fixture_id      INTEGER NOT NULL,
        ingested_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        raw_json        JSONB NOT NULL
    );

    CREATE TABLE IF NOT EXISTS bronze_statistics (
        id              SERIAL PRIMARY KEY,
        fixture_id      INTEGER NOT NULL,
        ingested_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        raw_json        JSONB NOT NULL
    );

    CREATE TABLE IF NOT EXISTS bronze_player_stats (
        id              SERIAL PRIMARY KEY,
        fixture_id      INTEGER NOT NULL,
        ingested_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        raw_json        JSONB NOT NULL
    );

    CREATE TABLE IF NOT EXISTS ingestion_watermark (
        table_name      VARCHAR(100) PRIMARY KEY,
        last_fixture_id INTEGER,
        last_run_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );
    """
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(ddl)
        conn.commit()


def fixture_already_ingested(fixture_id: int, table: str) -> bool:
    sql = f"SELECT 1 FROM {table} WHERE fixture_id = %s LIMIT 1"
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (fixture_id,))
        return cur.fetchone() is not None


def save_fixtures(fixtures: list[dict]):
    sql = """
    INSERT INTO bronze_fixtures (fixture_id, raw_json)
    VALUES (%s, %s)
    ON CONFLICT DO NOTHING
    """
    rows = [(f["fixture"]["id"], Json(f)) for f in fixtures]
    with _get_conn() as conn, conn.cursor() as cur:
        cur.executemany(sql, rows)
        conn.commit()
    _save_json_files("fixtures", {f["fixture"]["id"]: f for f in fixtures})


def save_lineups(fixture_id: int, lineups: list[dict]):
    sql = "INSERT INTO bronze_lineups (fixture_id, raw_json) VALUES (%s, %s)"
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (fixture_id, Json(lineups)))
        conn.commit()
    _save_json_files("lineups", {fixture_id: lineups})


def save_statistics(fixture_id: int, stats: list[dict]):
    sql = "INSERT INTO bronze_statistics (fixture_id, raw_json) VALUES (%s, %s)"
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (fixture_id, Json(stats)))
        conn.commit()
    _save_json_files("statistics", {fixture_id: stats})


def save_player_stats(fixture_id: int, player_stats: list[dict]):
    sql = "INSERT INTO bronze_player_stats (fixture_id, raw_json) VALUES (%s, %s)"
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (fixture_id, Json(player_stats)))
        conn.commit()
    _save_json_files("player_stats", {fixture_id: player_stats})


def update_watermark(table_name: str, last_fixture_id: int):
    sql = """
    INSERT INTO ingestion_watermark (table_name, last_fixture_id, last_run_at)
    VALUES (%s, %s, NOW())
    ON CONFLICT (table_name) DO UPDATE
        SET last_fixture_id = EXCLUDED.last_fixture_id,
            last_run_at = NOW()
    """
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (table_name, last_fixture_id))
        conn.commit()


def _save_json_files(category: str, data: dict):
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dir_path = os.path.join(BRONZE_DATA_DIR, category, date_str)
    os.makedirs(dir_path, exist_ok=True)
    for key, value in data.items():
        filepath = os.path.join(dir_path, f"{key}.json")
        with open(filepath, "w") as f:
            json.dump(value, f, ensure_ascii=False)
