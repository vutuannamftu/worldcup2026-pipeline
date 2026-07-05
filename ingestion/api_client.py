import time
import logging
import requests
from ingestion.config import API_FOOTBALL_KEY, API_FOOTBALL_BASE_URL

log = logging.getLogger(__name__)


class DailyLimitReached(Exception):
    """Raised khi API-Football báo hết quota ngày hôm nay."""
    pass


HEADERS = {
    "x-apisports-key": API_FOOTBALL_KEY,
}

# Free tier: 10 requests/minute, 100/day
REQUEST_DELAY = 7  # giây giữa mỗi request (7s = ~8.5 req/phút, an toàn hơn 6s)
MAX_RETRIES = 3


def _get(endpoint: str, params: dict) -> dict:
    url = f"{API_FOOTBALL_BASE_URL}/{endpoint}"

    for attempt in range(MAX_RETRIES):
        response = requests.get(url, headers=HEADERS, params=params, timeout=30)

        if response.status_code == 429:
            wait = 65  # đợi 65s để reset rate limit window
            log.warning(f"429 Too Many Requests on {endpoint}, waiting {wait}s (attempt {attempt+1}/{MAX_RETRIES})")
            time.sleep(wait)
            continue

        response.raise_for_status()
        data = response.json()

        errors = data.get("errors", {})
        if errors:
            error_str = str(errors)
            if "request limit" in error_str.lower() or "upgrade your plan" in error_str.lower():
                raise DailyLimitReached(f"Daily API quota exhausted on {endpoint}")
            raise ValueError(f"API error on {endpoint}: {errors}")

        time.sleep(REQUEST_DELAY)
        return data

    raise RuntimeError(f"Failed after {MAX_RETRIES} retries on {endpoint}")


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
