import logging
from ingestion import api_client, bronze
from ingestion.api_client import DailyLimitReached
from ingestion.config import WC_LEAGUE_ID, WC_SEASON

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Chỉ lấy dữ liệu chi tiết cho các trận đã kết thúc
FINISHED_STATUSES = {"FT", "AET", "PEN"}


def run_fixtures():
    """Lấy danh sách tất cả trận World Cup, lưu vào bronze_fixtures."""
    log.info("Fetching fixtures for WC%s...", WC_SEASON)
    try:
        fixtures = api_client.get_fixtures(WC_LEAGUE_ID, WC_SEASON)
    except DailyLimitReached:
        log.warning("Daily API quota exhausted while fetching fixtures. Will retry tomorrow.")
        return []
    log.info(f"Got {len(fixtures)} fixtures")
    bronze.save_fixtures(fixtures)
    log.info("Saved fixtures to bronze")
    return fixtures


def run_match_details(fixtures: list[dict] | None = None):
    """
    Với mỗi trận đã kết thúc chưa có dữ liệu chi tiết,
    lấy lineups + statistics + player stats.
    """
    if fixtures is None:
        try:
            fixtures = api_client.get_fixtures(WC_LEAGUE_ID, WC_SEASON)
        except DailyLimitReached:
            log.warning("Daily API quota exhausted while fetching fixtures. Will retry tomorrow.")
            return 0

    finished = [
        f for f in fixtures
        if f.get("fixture", {}).get("status", {}).get("short") in FINISHED_STATUSES
    ]
    log.info(f"{len(finished)} finished fixtures to process")

    processed = 0
    for fixture in finished:
        fixture_id = fixture["fixture"]["id"]

        if bronze.fixture_already_ingested(fixture_id, "bronze_lineups"):
            log.info(f"Fixture {fixture_id} already ingested, skipping")
            continue

        log.info(f"Processing fixture {fixture_id}...")

        try:
            lineups = api_client.get_lineups(fixture_id)
            bronze.save_lineups(fixture_id, lineups)

            stats = api_client.get_statistics(fixture_id)
            bronze.save_statistics(fixture_id, stats)

            player_stats = api_client.get_player_stats(fixture_id)
            bronze.save_player_stats(fixture_id, player_stats)

        except DailyLimitReached:
            log.warning(f"Daily API quota exhausted after {processed} fixtures. Will continue tomorrow.")
            break

        bronze.update_watermark("match_details", fixture_id)
        processed += 1
        log.info(f"Fixture {fixture_id} done ({processed} processed this run)")

    log.info(f"Ingestion complete. {processed} new fixtures processed.")
    return processed


def run_all():
    """Full ingestion run: fixtures + tất cả match details chưa có."""
    bronze.init_bronze_tables()
    fixtures = run_fixtures()
    run_match_details(fixtures)
