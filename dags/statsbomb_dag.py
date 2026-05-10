"""
StatsBomb Football Pipeline DAG
Orchestrates end-to-end ingestion:
  1. Bootstrap dimensions (competitions, matches, teams, players)
  2. Download raw event JSON → /shared/raw/{match_id}.json  (for NiFi → ES)
  3. Load fact tables into PostgreSQL (star schema)
  4. Refresh materialised views
  5. Signal NiFi to begin processing (trigger HTTP)
  6. Wait for Elasticsearch to reflect the loaded data
"""

import json
import os
import time
import logging
from datetime import datetime, timedelta

import requests
import psycopg2
import psycopg2.extras
import pandas as pd
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

# ── Connection helpers ────────────────────────────────────────────────────────

def _pg_conn():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )

# ── Competition / season targets ──────────────────────────────────────────────
COMPETITIONS = [
    {"competition_id": 55, "season_id": 282,
     "competition_name": "UEFA Euro", "season_name": "2024",
     "country_name": "Europe"},
    {"competition_id": 43, "season_id": 106,
     "competition_name": "FIFA World Cup", "season_name": "2022",
     "country_name": "World"},
]

# No match limit — ingest the full tournament data for both competitions
MATCH_LIMIT = None

# ── DAG definition ────────────────────────────────────────────────────────────

default_args = {
    "owner": "football-pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

@dag(
    dag_id="statsbomb_football_pipeline",
    description="End-to-end StatsBomb ingestion: PostgreSQL + Elasticsearch via NiFi",
    schedule="@once",
    start_date=days_ago(1),
    catchup=False,
    tags=["statsbomb", "football", "etl"],
    default_args=default_args,
)
def statsbomb_pipeline():

    # ── Task 1: Prepare shared filesystem ────────────────────────────────────
    @task
    def setup_directories():
        for d in ["/shared/raw", "/shared/processed", "/shared/nifi_trigger"]:
            os.makedirs(d, exist_ok=True)
        log.info("Shared directories ready.")

    # ── Task 2: Load dimension tables ─────────────────────────────────────────
    @task
    def load_dimensions():
        """Populate dim_competitions and dim_matches from StatsBomb's matches JSON."""
        import urllib.request

        raw_base = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"
        conn = _pg_conn()
        cur = conn.cursor()

        for comp in COMPETITIONS:
            cur.execute("""
                INSERT INTO dim_competitions
                    (competition_id, competition_name, season_id, season_name, country_name)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (competition_id) DO UPDATE
                    SET competition_name = EXCLUDED.competition_name,
                        season_name      = EXCLUDED.season_name;
            """, (comp["competition_id"], comp["competition_name"],
                  comp["season_id"],     comp["season_name"],
                  comp["country_name"]))

            matches_url = f"{raw_base}/matches/{comp['competition_id']}/{comp['season_id']}.json"
            with urllib.request.urlopen(matches_url, timeout=30) as r:
                matches = json.loads(r.read())

            for m in matches:
                home_team = m.get("home_team") or {}
                away_team = m.get("away_team") or {}
                stadium   = m.get("stadium")   or {}
                referee   = m.get("referee")   or {}

                cur.execute("""
                    INSERT INTO dim_matches (
                        match_id, competition_id, match_date,
                        home_team_id, home_team_name,
                        away_team_id, away_team_name,
                        home_score, away_score,
                        stadium_name, referee_name
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (match_id) DO NOTHING;
                """, (
                    int(m["match_id"]),
                    comp["competition_id"],
                    m.get("match_date"),
                    int(home_team.get("home_team_id") or 0),
                    home_team.get("home_team_name") or "",
                    int(away_team.get("away_team_id") or 0),
                    away_team.get("away_team_name") or "",
                    int(m.get("home_score") or 0),
                    int(m.get("away_score") or 0),
                    stadium.get("name") if isinstance(stadium, dict) else None,
                    referee.get("name") if isinstance(referee, dict) else None,
                ))

            log.info("Loaded %d matches for %s", len(matches), comp["competition_name"])

        conn.commit()
        cur.close()
        conn.close()

    # ── Task 3: Download raw events (for NiFi → ES path) ─────────────────────
    @task
    def download_raw_events():
        """
        Download raw StatsBomb event JSON from GitHub and write to /shared/raw/.
        NiFi watches this directory and routes events to Elasticsearch.
        """
        import urllib.request

        raw_base = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"

        for comp in COMPETITIONS:
            matches_url = f"{raw_base}/matches/{comp['competition_id']}/{comp['season_id']}.json"
            with urllib.request.urlopen(matches_url, timeout=30) as r:
                matches = json.loads(r.read())[:MATCH_LIMIT]

            for m in matches:
                match_id = m["match_id"]
                out_path = f"/shared/raw/{match_id}.json"
                if os.path.exists(out_path):
                    continue  # idempotent — skip already-downloaded matches

                events_url = f"{raw_base}/events/{match_id}.json"
                try:
                    with urllib.request.urlopen(events_url, timeout=60) as r:
                        raw_events = json.loads(r.read())
                    # Inject match metadata so NiFi can propagate it
                    for ev in raw_events:
                        ev["match_id"] = match_id
                        ev["competition_name"] = comp["competition_name"]
                        ev["season_name"] = comp["season_name"]
                    with open(out_path, "w") as f:
                        json.dump(raw_events, f)
                    log.info("Downloaded match %s (%d events)", match_id, len(raw_events))
                except Exception as exc:
                    log.warning("Could not download match %s: %s", match_id, exc)

    # ── Task 4: Load PostgreSQL fact tables ───────────────────────────────────
    @task
    def load_fact_tables():
        """
        Load fact_events / fact_passes / fact_shots and the dimension tables that
        depend on event content (dim_players, dim_teams, dim_event_types).

        We parse the raw StatsBomb JSON from GitHub directly because statsbombpy
        with flatten_attrs=True strips IDs from nested player/team/type objects.
        """
        import urllib.request

        raw_base = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"
        conn = _pg_conn()
        cur = conn.cursor()

        cur.execute("SELECT match_id FROM dim_matches ORDER BY match_id;")
        all_match_ids = [r[0] for r in cur.fetchall()]

        for match_id in all_match_ids:
            # Idempotency: skip already-loaded matches
            cur.execute("SELECT 1 FROM fact_events WHERE match_id = %s LIMIT 1", (match_id,))
            if cur.fetchone():
                continue

            events_url = f"{raw_base}/events/{match_id}.json"
            try:
                with urllib.request.urlopen(events_url, timeout=60) as r:
                    events = json.loads(r.read())
            except Exception as exc:
                log.warning("Could not download events for match %s: %s", match_id, exc)
                continue

            players, teams, event_types = {}, {}, {}
            event_rows, pass_rows, shot_rows = [], [], []

            for ev in events:
                ev_id = ev.get("id")
                if not ev_id:
                    continue

                player = ev.get("player") or {}
                team   = ev.get("team")   or {}
                type_  = ev.get("type")   or {}

                player_id = player.get("id")   if isinstance(player, dict) else None
                team_id   = team.get("id")     if isinstance(team, dict)   else None
                type_id   = type_.get("id")    if isinstance(type_, dict)  else None

                if player_id and player.get("name"):
                    players[player_id] = player["name"]
                if team_id and team.get("name"):
                    teams[team_id] = team["name"]
                if type_id and type_.get("name"):
                    event_types[type_id] = type_["name"]

                loc = ev.get("location") or []
                lx = float(loc[0]) if len(loc) >= 2 else None
                ly = float(loc[1]) if len(loc) >= 2 else None

                event_rows.append((
                    ev_id,
                    int(ev.get("index", 0)),
                    match_id,
                    team_id,
                    player_id,
                    type_id,
                    int(ev.get("period", 0)),
                    int(ev.get("minute", 0)),
                    int(ev.get("second", 0)),
                    str(ev.get("timestamp", "")),
                    lx, ly,
                    float(ev["duration"]) if ev.get("duration") is not None else None,
                    int(ev.get("possession", 0)),
                    bool(ev.get("under_pressure", False)),
                ))

                # Pass sub-event
                if isinstance(ev.get("pass"), dict):
                    p = ev["pass"]
                    recipient = p.get("recipient") or {}
                    recipient_id = recipient.get("id") if isinstance(recipient, dict) else None
                    if recipient_id and recipient.get("name"):
                        players[recipient_id] = recipient["name"]

                    end_loc = p.get("end_location") or []
                    ex = float(end_loc[0]) if len(end_loc) >= 2 else None
                    ey = float(end_loc[1]) if len(end_loc) >= 2 else None

                    outcome  = p.get("outcome")   or {}
                    height   = p.get("height")    or {}
                    bp       = p.get("body_part") or {}
                    ptype    = p.get("type")      or {}

                    pass_rows.append((
                        ev_id,
                        recipient_id,
                        p.get("length"),
                        p.get("angle"),
                        ex, ey,
                        height.get("name")  if isinstance(height, dict)  else None,
                        bp.get("name")      if isinstance(bp, dict)      else None,
                        ptype.get("name")   if isinstance(ptype, dict)   else None,
                        outcome.get("name") if isinstance(outcome, dict) else None,
                        bool(p.get("cross", False)),
                        bool(p.get("through_ball", False)),
                        bool(p.get("switch", False)),
                    ))

                # Shot sub-event
                if isinstance(ev.get("shot"), dict):
                    s = ev["shot"]
                    end_loc = s.get("end_location") or []
                    ex = float(end_loc[0]) if len(end_loc) >= 1 else None
                    ey = float(end_loc[1]) if len(end_loc) >= 2 else None
                    ez = float(end_loc[2]) if len(end_loc) >= 3 else None

                    outcome   = s.get("outcome")   or {}
                    technique = s.get("technique") or {}
                    bp        = s.get("body_part") or {}

                    shot_rows.append((
                        ev_id,
                        s.get("statsbomb_xg"),
                        ex, ey, ez,
                        outcome.get("name")   if isinstance(outcome, dict)   else None,
                        technique.get("name") if isinstance(technique, dict) else None,
                        bp.get("name")        if isinstance(bp, dict)        else None,
                        bool(s.get("first_time", False)),
                        bool(s.get("one_on_one", False)),
                    ))

            # Insert dims first (FK targets), then facts
            if players:
                psycopg2.extras.execute_batch(cur, """
                    INSERT INTO dim_players (player_id, player_name)
                    VALUES (%s, %s)
                    ON CONFLICT (player_id) DO NOTHING;
                """, list(players.items()), page_size=500)
            if teams:
                psycopg2.extras.execute_batch(cur, """
                    INSERT INTO dim_teams (team_id, team_name)
                    VALUES (%s, %s)
                    ON CONFLICT (team_id) DO NOTHING;
                """, list(teams.items()), page_size=500)
            if event_types:
                psycopg2.extras.execute_batch(cur, """
                    INSERT INTO dim_event_types (type_id, type_name)
                    VALUES (%s, %s)
                    ON CONFLICT (type_id) DO NOTHING;
                """, list(event_types.items()), page_size=500)

            psycopg2.extras.execute_batch(cur, """
                INSERT INTO fact_events (
                    event_uuid, event_index, match_id, team_id, player_id, type_id,
                    period, minute, second, timestamp,
                    location_x, location_y, duration, possession, under_pressure
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (match_id, event_index) DO NOTHING;
            """, event_rows, page_size=500)

            if pass_rows:
                psycopg2.extras.execute_batch(cur, """
                    INSERT INTO fact_passes (
                        event_uuid, recipient_id, pass_length, pass_angle,
                        pass_end_x, pass_end_y, pass_height, pass_body_part,
                        pass_type, pass_outcome, is_cross, through_ball, switch
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (event_uuid) DO NOTHING;
                """, pass_rows, page_size=500)

            if shot_rows:
                psycopg2.extras.execute_batch(cur, """
                    INSERT INTO fact_shots (
                        event_uuid, shot_xg, shot_end_x, shot_end_y, shot_end_z,
                        shot_outcome, shot_technique, shot_body_part,
                        first_time, one_on_one
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (event_uuid) DO NOTHING;
                """, shot_rows, page_size=500)

            conn.commit()
            log.info("Loaded match %s: %d events", match_id, len(event_rows))

        cur.close()
        conn.close()

    # ── Task 5: Refresh materialised views ───────────────────────────────────
    @task
    def refresh_views():
        conn = _pg_conn()
        cur = conn.cursor()
        cur.execute("REFRESH MATERIALIZED VIEW mv_match_summary;")
        cur.execute("REFRESH MATERIALIZED VIEW mv_player_stats;")
        conn.commit()
        cur.close()
        conn.close()
        log.info("Materialised views refreshed.")

    # ── Task 6: Trigger NiFi to process raw files ─────────────────────────────
    @task
    def trigger_nifi():
        """
        Write a sentinel file to /shared/nifi_trigger/ so NiFi's GetFile
        processor starts the Elasticsearch ingestion path.
        """
        # NiFi watches /shared/raw/ directly via GetFile.
        # We also ping the NiFi API to ensure the flow is running.
        nifi_url = os.environ.get("NIFI_URL", "http://nifi:8080")
        nifi_user = os.environ.get("NIFI_USER", "nifi_admin")
        nifi_password = os.environ.get("NIFI_PASSWORD", "nifi_admin12345")

        try:
            token_resp = requests.post(
                f"{nifi_url}/nifi-api/access/token",
                data={"username": nifi_user, "password": nifi_password},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=15,
            )
            token = token_resp.text.strip()
            headers = {"Authorization": f"Bearer {token}"}

            diagnostics = requests.get(
                f"{nifi_url}/nifi-api/system-diagnostics",
                headers=headers, timeout=10
            )
            log.info("NiFi system diagnostics status: %s", diagnostics.status_code)
        except Exception as exc:
            log.warning("Could not reach NiFi API: %s — files already in /shared/raw/", exc)

        # Write trigger sentinel
        trigger_path = "/shared/nifi_trigger/ready.flag"
        with open(trigger_path, "w") as f:
            f.write(datetime.utcnow().isoformat())
        log.info("NiFi trigger written to %s", trigger_path)

    # ── Task 7: Verify Elasticsearch has data ────────────────────────────────
    @task
    def verify_elasticsearch():
        es_host = os.environ.get("ES_HOST", "elasticsearch")
        es_port = os.environ.get("ES_PORT", "9200")
        es_index = os.environ.get("ES_INDEX", "football_events")

        for attempt in range(12):
            try:
                r = requests.get(
                    f"http://{es_host}:{es_port}/{es_index}/_count",
                    timeout=10
                )
                if r.status_code == 200:
                    count = r.json().get("count", 0)
                    log.info("Elasticsearch index '%s' has %d documents.", es_index, count)
                    if count > 0:
                        return {"es_doc_count": count}
            except Exception as exc:
                log.debug("ES check attempt %d failed: %s", attempt, exc)
            time.sleep(30)

        log.warning("Elasticsearch index may still be empty — NiFi may need more time.")
        return {"es_doc_count": 0}

    # ── Wire tasks ────────────────────────────────────────────────────────────
    dirs      = setup_directories()
    dims      = load_dimensions()
    raw       = download_raw_events()
    facts     = load_fact_tables()
    views     = refresh_views()
    nifi_trig = trigger_nifi()
    es_check  = verify_elasticsearch()

    dirs >> [dims, raw]
    dims >> facts >> views
    raw >> nifi_trig >> es_check


statsbomb_pipeline()
