import logging
import psycopg2
from psycopg2.extras import execute_values
from ingestion.config import DB_CONFIG

log = logging.getLogger(__name__)


def _get_conn():
    return psycopg2.connect(**DB_CONFIG)


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

def init_silver_tables():
    ddl = """
    CREATE TABLE IF NOT EXISTS silver_fixtures (
        fixture_id          INTEGER PRIMARY KEY,
        date                TIMESTAMP WITH TIME ZONE,
        season              INTEGER,
        round               TEXT,
        venue_id            INTEGER,
        venue_name          TEXT,
        venue_city          TEXT,
        referee             TEXT,
        status_short        TEXT,
        status_long         TEXT,
        home_team_id        INTEGER,
        home_team_name      TEXT,
        away_team_id        INTEGER,
        away_team_name      TEXT,
        home_goals          INTEGER,
        away_goals          INTEGER,
        home_goals_ht       INTEGER,
        away_goals_ht       INTEGER,
        home_goals_et       INTEGER,
        away_goals_et       INTEGER,
        home_goals_pen      INTEGER,
        away_goals_pen      INTEGER,
        winner              VARCHAR(10),   -- 'home', 'away', 'draw'
        inserted_at         TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS silver_teams (
        team_id             INTEGER PRIMARY KEY,
        team_name           TEXT NOT NULL,
        inserted_at         TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS silver_lineups (
        fixture_id          INTEGER,
        team_id             INTEGER,
        team_name           TEXT,
        formation           TEXT,
        coach_id            INTEGER,
        coach_name          TEXT,
        inserted_at         TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        PRIMARY KEY (fixture_id, team_id)
    );

    CREATE TABLE IF NOT EXISTS silver_lineup_players (
        fixture_id          INTEGER,
        team_id             INTEGER,
        player_id           INTEGER,
        player_name         TEXT,
        player_number       INTEGER,
        position            TEXT,
        grid                TEXT,
        is_substitute       BOOLEAN,
        inserted_at         TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        PRIMARY KEY (fixture_id, team_id, player_id)
    );

    CREATE TABLE IF NOT EXISTS silver_team_stats (
        fixture_id          INTEGER,
        team_id             INTEGER,
        team_name           TEXT,
        shots_on_goal       INTEGER,
        shots_off_goal      INTEGER,
        total_shots         INTEGER,
        blocked_shots       INTEGER,
        shots_inside_box    INTEGER,
        shots_outside_box   INTEGER,
        fouls               INTEGER,
        corner_kicks        INTEGER,
        offsides            INTEGER,
        ball_possession     INTEGER,   -- % without sign
        yellow_cards        INTEGER,
        red_cards           INTEGER,
        goalkeeper_saves    INTEGER,
        total_passes        INTEGER,
        accurate_passes     INTEGER,
        passes_pct          INTEGER,
        expected_goals      NUMERIC(6,2),
        inserted_at         TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        PRIMARY KEY (fixture_id, team_id)
    );

    CREATE TABLE IF NOT EXISTS silver_player_stats (
        fixture_id          INTEGER,
        team_id             INTEGER,
        player_id           INTEGER,
        player_name         TEXT,
        minutes_played      INTEGER,
        position            TEXT,
        rating              NUMERIC(4,2),
        captain             BOOLEAN,
        substitute          BOOLEAN,
        shots_total         INTEGER,
        shots_on_goal       INTEGER,
        goals_scored        INTEGER,
        goals_conceded      INTEGER,
        assists             INTEGER,
        saves               INTEGER,
        passes_total        INTEGER,
        passes_key          INTEGER,
        passes_accuracy     INTEGER,
        tackles_total       INTEGER,
        tackles_blocks      INTEGER,
        tackles_interceptions INTEGER,
        duels_total         INTEGER,
        duels_won           INTEGER,
        dribbles_attempts   INTEGER,
        dribbles_success    INTEGER,
        fouls_drawn         INTEGER,
        fouls_committed     INTEGER,
        yellow_cards        INTEGER,
        red_cards           INTEGER,
        penalties_won       INTEGER,
        penalties_committed INTEGER,
        penalties_scored    INTEGER,
        penalties_missed    INTEGER,
        penalties_saved     INTEGER,
        inserted_at         TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        PRIMARY KEY (fixture_id, team_id, player_id)
    );
    """
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(ddl)
        conn.commit()
    log.info("Silver tables initialized")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_int(val):
    """Convert '45%' or None or int → int or None."""
    if val is None:
        return None
    if isinstance(val, int):
        return val
    if isinstance(val, float):
        return int(val)
    s = str(val).strip().rstrip("%")
    try:
        return int(s)
    except (ValueError, TypeError):
        return None


def _parse_float(val):
    if val is None:
        return None
    try:
        return float(str(val).strip())
    except (ValueError, TypeError):
        return None


def _stat_map(statistics: list[dict]) -> dict:
    """Convert [{'type': 'Shots on Goal', 'value': 1}, ...] → dict keyed by type."""
    return {s["type"]: s.get("value") for s in statistics}


# ---------------------------------------------------------------------------
# Transformations
# ---------------------------------------------------------------------------

def transform_fixtures() -> int:
    """bronze_fixtures → silver_fixtures + silver_teams."""
    select_sql = """
        SELECT DISTINCT ON (fixture_id) fixture_id, raw_json
        FROM bronze_fixtures
        ORDER BY fixture_id, ingested_at DESC
    """
    upsert_fixture = """
        INSERT INTO silver_fixtures (
            fixture_id, date, season, round,
            venue_id, venue_name, venue_city, referee,
            status_short, status_long,
            home_team_id, home_team_name,
            away_team_id, away_team_name,
            home_goals, away_goals,
            home_goals_ht, away_goals_ht,
            home_goals_et, away_goals_et,
            home_goals_pen, away_goals_pen,
            winner
        ) VALUES %s
        ON CONFLICT (fixture_id) DO UPDATE SET
            home_goals = EXCLUDED.home_goals,
            away_goals = EXCLUDED.away_goals,
            status_short = EXCLUDED.status_short,
            status_long  = EXCLUDED.status_long,
            winner       = EXCLUDED.winner
    """
    upsert_team = """
        INSERT INTO silver_teams (team_id, team_name)
        VALUES %s
        ON CONFLICT (team_id) DO NOTHING
    """

    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(select_sql)
        rows = cur.fetchall()

    fixture_rows = []
    team_rows = []
    seen_teams = set()

    for _, raw in rows:
        f = raw.get("fixture", {})
        league = raw.get("league", {})
        teams = raw.get("teams", {})
        goals = raw.get("goals", {})
        score = raw.get("score", {})

        home = teams.get("home", {})
        away = teams.get("away", {})
        venue = f.get("venue", {})

        if home.get("winner"):
            winner = "home"
        elif away.get("winner"):
            winner = "away"
        else:
            winner = "draw" if f.get("status", {}).get("short") in {"FT", "AET", "PEN"} else None

        fixture_rows.append((
            f["id"],
            f.get("date"),
            league.get("season"),
            league.get("round"),
            venue.get("id"),
            venue.get("name"),
            venue.get("city"),
            f.get("referee"),
            f.get("status", {}).get("short"),
            f.get("status", {}).get("long"),
            home.get("id"),
            home.get("name"),
            away.get("id"),
            away.get("name"),
            goals.get("home"),
            goals.get("away"),
            score.get("halftime", {}).get("home"),
            score.get("halftime", {}).get("away"),
            score.get("extratime", {}).get("home"),
            score.get("extratime", {}).get("away"),
            score.get("penalty", {}).get("home"),
            score.get("penalty", {}).get("away"),
            winner,
        ))

        for team in [home, away]:
            if team.get("id") and team["id"] not in seen_teams:
                team_rows.append((team["id"], team["name"]))
                seen_teams.add(team["id"])

    with _get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, upsert_fixture, fixture_rows)
        if team_rows:
            execute_values(cur, upsert_team, team_rows)
        conn.commit()

    log.info("transform_fixtures: %d fixtures, %d teams", len(fixture_rows), len(team_rows))
    return len(fixture_rows)


def transform_lineups() -> int:
    """bronze_lineups → silver_lineups + silver_lineup_players."""
    select_sql = """
        SELECT DISTINCT ON (fixture_id) fixture_id, raw_json
        FROM bronze_lineups
        ORDER BY fixture_id, ingested_at DESC
    """
    upsert_lineup = """
        INSERT INTO silver_lineups (fixture_id, team_id, team_name, formation, coach_id, coach_name)
        VALUES %s
        ON CONFLICT (fixture_id, team_id) DO NOTHING
    """
    upsert_players = """
        INSERT INTO silver_lineup_players
            (fixture_id, team_id, player_id, player_name, player_number, position, grid, is_substitute)
        VALUES %s
        ON CONFLICT (fixture_id, team_id, player_id) DO NOTHING
    """

    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(select_sql)
        rows = cur.fetchall()

    lineup_rows = []
    player_rows = []

    for fixture_id, raw in rows:
        team_list = raw if isinstance(raw, list) else []
        for team_data in team_list:
            team = team_data.get("team", {})
            coach = team_data.get("coach", {})
            team_id = team.get("id")

            lineup_rows.append((
                fixture_id,
                team_id,
                team.get("name"),
                team_data.get("formation"),
                coach.get("id"),
                coach.get("name"),
            ))

            for entry in team_data.get("startXI", []):
                p = entry.get("player", {})
                player_rows.append((
                    fixture_id, team_id,
                    p.get("id"), p.get("name"), p.get("number"),
                    p.get("pos"), p.get("grid"), False
                ))

            for entry in team_data.get("substitutes", []):
                p = entry.get("player", {})
                player_rows.append((
                    fixture_id, team_id,
                    p.get("id"), p.get("name"), p.get("number"),
                    p.get("pos"), p.get("grid"), True
                ))

    with _get_conn() as conn, conn.cursor() as cur:
        if lineup_rows:
            execute_values(cur, upsert_lineup, lineup_rows)
        if player_rows:
            execute_values(cur, upsert_players, player_rows)
        conn.commit()

    log.info("transform_lineups: %d lineup entries, %d player entries", len(lineup_rows), len(player_rows))
    return len(lineup_rows)


def transform_team_stats() -> int:
    """bronze_statistics → silver_team_stats."""
    select_sql = """
        SELECT DISTINCT ON (fixture_id) fixture_id, raw_json
        FROM bronze_statistics
        ORDER BY fixture_id, ingested_at DESC
    """
    upsert_sql = """
        INSERT INTO silver_team_stats (
            fixture_id, team_id, team_name,
            shots_on_goal, shots_off_goal, total_shots, blocked_shots,
            shots_inside_box, shots_outside_box,
            fouls, corner_kicks, offsides,
            ball_possession, yellow_cards, red_cards,
            goalkeeper_saves, total_passes, accurate_passes, passes_pct,
            expected_goals
        ) VALUES %s
        ON CONFLICT (fixture_id, team_id) DO NOTHING
    """

    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(select_sql)
        rows = cur.fetchall()

    stat_rows = []
    for fixture_id, raw in rows:
        team_list = raw if isinstance(raw, list) else []
        for team_data in team_list:
            team = team_data.get("team", {})
            sm = _stat_map(team_data.get("statistics", []))

            stat_rows.append((
                fixture_id,
                team.get("id"),
                team.get("name"),
                _parse_int(sm.get("Shots on Goal")),
                _parse_int(sm.get("Shots off Goal")),
                _parse_int(sm.get("Total Shots")),
                _parse_int(sm.get("Blocked Shots")),
                _parse_int(sm.get("Shots insidebox")),
                _parse_int(sm.get("Shots outsidebox")),
                _parse_int(sm.get("Fouls")),
                _parse_int(sm.get("Corner Kicks")),
                _parse_int(sm.get("Offsides")),
                _parse_int(sm.get("Ball Possession")),
                _parse_int(sm.get("Yellow Cards")),
                _parse_int(sm.get("Red Cards")),
                _parse_int(sm.get("Goalkeeper Saves")),
                _parse_int(sm.get("Total passes")),
                _parse_int(sm.get("Passes accurate")),
                _parse_int(sm.get("Passes %")),
                _parse_float(sm.get("expected_goals")),
            ))

    with _get_conn() as conn, conn.cursor() as cur:
        if stat_rows:
            execute_values(cur, upsert_sql, stat_rows)
        conn.commit()

    log.info("transform_team_stats: %d rows", len(stat_rows))
    return len(stat_rows)


def transform_player_stats() -> int:
    """bronze_player_stats → silver_player_stats."""
    select_sql = """
        SELECT DISTINCT ON (fixture_id) fixture_id, raw_json
        FROM bronze_player_stats
        ORDER BY fixture_id, ingested_at DESC
    """
    upsert_sql = """
        INSERT INTO silver_player_stats (
            fixture_id, team_id, player_id, player_name,
            minutes_played, position, rating, captain, substitute,
            shots_total, shots_on_goal,
            goals_scored, goals_conceded, assists, saves,
            passes_total, passes_key, passes_accuracy,
            tackles_total, tackles_blocks, tackles_interceptions,
            duels_total, duels_won,
            dribbles_attempts, dribbles_success,
            fouls_drawn, fouls_committed,
            yellow_cards, red_cards,
            penalties_won, penalties_committed,
            penalties_scored, penalties_missed, penalties_saved
        ) VALUES %s
        ON CONFLICT (fixture_id, team_id, player_id) DO NOTHING
    """

    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(select_sql)
        rows = cur.fetchall()

    player_rows = []
    for fixture_id, raw in rows:
        team_list = raw if isinstance(raw, list) else []
        for team_data in team_list:
            team = team_data.get("team", {})
            team_id = team.get("id")

            for entry in team_data.get("players", []):
                player = entry.get("player", {})
                st = (entry.get("statistics") or [{}])[0]

                games = st.get("games", {})
                shots = st.get("shots", {})
                goals_d = st.get("goals", {})
                passes = st.get("passes", {})
                tackles = st.get("tackles", {})
                duels = st.get("duels", {})
                dribbles = st.get("dribbles", {})
                fouls = st.get("fouls", {})
                cards = st.get("cards", {})
                pen = st.get("penalty", {})

                rating_raw = games.get("rating")
                rating = _parse_float(rating_raw)

                player_rows.append((
                    fixture_id, team_id,
                    player.get("id"), player.get("name"),
                    _parse_int(games.get("minutes")),
                    games.get("position"),
                    rating,
                    bool(games.get("captain")),
                    bool(games.get("substitute")),
                    _parse_int(shots.get("total")),
                    _parse_int(shots.get("on")),
                    _parse_int(goals_d.get("total")),
                    _parse_int(goals_d.get("conceded")),
                    _parse_int(goals_d.get("assists")),
                    _parse_int(goals_d.get("saves")),
                    _parse_int(passes.get("total")),
                    _parse_int(passes.get("key")),
                    _parse_int(passes.get("accuracy")),
                    _parse_int(tackles.get("total")),
                    _parse_int(tackles.get("blocks")),
                    _parse_int(tackles.get("interceptions")),
                    _parse_int(duels.get("total")),
                    _parse_int(duels.get("won")),
                    _parse_int(dribbles.get("attempts")),
                    _parse_int(dribbles.get("success")),
                    _parse_int(fouls.get("drawn")),
                    _parse_int(fouls.get("committed")),
                    _parse_int(cards.get("yellow")),
                    _parse_int(cards.get("red")),
                    _parse_int(pen.get("won")),
                    _parse_int(pen.get("commited")),
                    _parse_int(pen.get("scored")),
                    _parse_int(pen.get("missed")),
                    _parse_int(pen.get("saved")),
                ))

    with _get_conn() as conn, conn.cursor() as cur:
        if player_rows:
            execute_values(cur, upsert_sql, player_rows)
        conn.commit()

    log.info("transform_player_stats: %d player rows", len(player_rows))
    return len(player_rows)


def run_all_silver():
    """Run all Silver transformations in order."""
    init_silver_tables()
    transform_fixtures()
    transform_lineups()
    transform_team_stats()
    transform_player_stats()
