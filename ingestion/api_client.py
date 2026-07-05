import time
import requests
from ingestion.config import API_FOOTBALL_KEY, API_FOOTBALL_BASE_URL

HEADERS = {
    "x-rapidapi-key": API_FOOTBALL_KEY,
    "x-rapidapi-host": "v3.football.api-sports.io",
}

# API-Football free tier: 10 requests/minute, 100/day
REQUEST_DELAY = 6  # giây giữa mỗi request để tránh rate limit


def _get(endpoint: str, params: dict) -> dict:
    url = f"{API_FOOTBALL_BASE_URL}/{endpoint}"
    response = requests.get(url, headers=HEADERS, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    errors = data.get("errors", {})
    if errors:
        raise ValueError(f"API error on {endpoint}: {errors}")

    time.sleep(REQUEST_DELAY)
    return data


def get_fixtures(league_id: int, season: int) -> list[dict]:
    data = _get("fixtures", {"league": league_id, "season": season})
    return data.get("response", [])


def get_lineups(fixture_id: int) -> list[dict]:
    data = _get("fixtures/lineups", {"fixture": fixture_id})
    return data.get("response", [])


def get_statistics(fixture_id: int) -> list[dict]:
    data = _get("fixtures/statistics", {"fixture": fixture_id})
    return data.get("response", [])


def get_player_stats(fixture_id: int) -> list[dict]:
    data = _get("fixtures/players", {"fixture": fixture_id})
    return data.get("response", [])
