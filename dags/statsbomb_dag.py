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
        """Populate dim_competitions, dim_matches, dim_teams, dim_players."""
        from statsbombpy import sb

        conn = _pg_conn()
        cur = conn.cursor()

        for comp in COMPETITIONS:
            # Upsert competition
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

            matches = sb.matches(
                competition_id=comp["competition_id"],
                season_id=comp["season_id"],
            ).head(MATCH_LIMIT)

            for _, m in matches.iterrows():
                # statsbombpy 1.0.x: home_team/away_team are dicts
                ht = m["home_team"] if isinstance(m.get("home_team"), dict) else {}
                at = m["away_team"] if isinstance(m.get("away_team"), dict) else {}
                home_team_id   = int(ht.get("home_team_id",   m.get("home_team_id",   0)))
                home_team_name = ht.get("home_team_name",  m.get("home_team_name",  ""))
                away_team_id   = int(at.get("away_team_id",   m.get("away_team_id",   0)))
                away_team_name = at.get("away_team_name",  m.get("away_team_name",  ""))

                # Teams
                for tid, tname in [(home_team_id, home_team_name), (away_team_id, away_team_name)]:
                    cur.execute("""
                        INSERT INTO dim_teams (team_id, team_name)
                        VALUES (%s, %s)
                        ON CONFLICT (team_id) DO NOTHING;
                    """, (tid, tname))

                stadium = m.get("stadium")
                referee = m.get("referee")

                # Match
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
                    home_team_id, home_team_name,
                    away_team_id, away_team_name,
                    int(m.get("home_score", 0)),
                    int(m.get("away_score", 0)),
                    stadium.get("name") if isinstance(stadium, dict) else None,
                    referee.get("name") if isinstance(referee, dict) else None,
                ))

        conn.commit()
        cur.close()
        conn.close()
        log.info("Dimensions loaded successfully.")

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
        """Load fact_events, fact_passes, fact_shots from statsbombpy DataFrames."""
        from statsbombpy import sb

        conn = _pg_conn()
        cur = conn.cursor()

        for comp in COMPETITIONS:
            matches = sb.matches(
                competition_id=comp["competition_id"],
                season_id=comp["season_id"],
            ).head(MATCH_LIMIT)

            for _, m in matches.iterrows():
                match_id = int(m["match_id"])

                # Skip if already loaded (idempotency key = match_id)
                cur.execute("SELECT 1 FROM fact_events WHERE match_id = %s LIMIT 1", (match_id,))
                if cur.fetchone():
                    continue

                try:
                    events = sb.events(match_id=match_id, split=False, flatten_attrs=True)
                except Exception as exc:
                    log.warning("Could not load events for match %s: %s", match_id, exc)
                    continue

                if events.empty:
                    continue

                # Upsert players
                if "player_id" in events.columns:
                    players = (
                        events[["player_id", "player"]].dropna(subset=["player_id"])
                        .drop_duplicates("player_id")
                    )
                    for _, row in players.iterrows():
                        cur.execute("""
                            INSERT INTO dim_players (player_id, player_name)
                            VALUES (%s, %s)
                            ON CONFLICT (player_id) DO NOTHING;
                        """, (int(row["player_id"]), str(row["player"])))

                # Upsert event types
                if "type_id" in events.columns:
                    types = (
                        events[["type_id", "type"]].dropna(subset=["type_id"])
                        .drop_duplicates("type_id")
                    )
                    for _, row in types.iterrows():
                        cur.execute("""
                            INSERT INTO dim_event_types (type_id, type_name)
                            VALUES (%s, %s)
                            ON CONFLICT (type_id) DO NOTHING;
                        """, (int(row["type_id"]), str(row["type"])))

                # ── fact_events ──
                event_rows = []
                for _, ev in events.iterrows():
                    loc = ev.get("location")
                    lx = float(loc[0]) if isinstance(loc, list) and len(loc) >= 2 else None
                    ly = float(loc[1]) if isinstance(loc, list) and len(loc) >= 2 else None
                    event_rows.append((
                        str(ev["id"]),
                        int(ev.get("index", 0)),
                        match_id,
                        int(ev["team_id"]) if pd.notna(ev.get("team_id")) else None,
                        int(ev["player_id"]) if pd.notna(ev.get("player_id")) else None,
                        int(ev["type_id"]) if pd.notna(ev.get("type_id")) else None,
                        int(ev.get("period", 0)),
                        int(ev.get("minute", 0)),
                        int(ev.get("second", 0)),
                        str(ev.get("timestamp", "")),
                        lx, ly,
                        float(ev["duration"]) if pd.notna(ev.get("duration")) else None,
                        int(ev.get("possession", 0)) if pd.notna(ev.get("possession")) else None,
                        bool(ev.get("under_pressure", False)),
                    ))

                psycopg2.extras.execute_batch(cur, """
                    INSERT INTO fact_events (
                        event_uuid, event_index, match_id, team_id, player_id, type_id,
                        period, minute, second, timestamp,
                        location_x, location_y, duration, possession, under_pressure
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (match_id, event_index) DO NOTHING;
                """, event_rows, page_size=500)

                # ── fact_passes ──
                pass_events = events[events["type"] == "Pass"] if "type" in events.columns else pd.DataFrame()
                pass_rows = []
                for _, ev in pass_events.iterrows():
                    end_loc = ev.get("pass_end_location")
                    ex = float(end_loc[0]) if isinstance(end_loc, list) and len(end_loc) >= 2 else None
                    ey = float(end_loc[1]) if isinstance(end_loc, list) and len(end_loc) >= 2 else None
                    pass_rows.append((
                        str(ev["id"]),
                        int(ev["pass_recipient_id"]) if pd.notna(ev.get("pass_recipient_id")) else None,
                        float(ev["pass_length"]) if pd.notna(ev.get("pass_length")) else None,
                        float(ev["pass_angle"]) if pd.notna(ev.get("pass_angle")) else None,
                        ex, ey,
                        str(ev.get("pass_height", "")) or None,
                        str(ev.get("pass_body_part", "")) or None,
                        str(ev.get("pass_type", "")) or None,
                        str(ev.get("pass_outcome", "")) or None,
                        bool(ev.get("pass_cross", False)),
                        bool(ev.get("pass_through_ball", False)),
                        bool(ev.get("pass_switch", False)),
                    ))
                if pass_rows:
                    psycopg2.extras.execute_batch(cur, """
                        INSERT INTO fact_passes (
                            event_uuid, recipient_id, pass_length, pass_angle,
                            pass_end_x, pass_end_y, pass_height, pass_body_part,
                            pass_type, pass_outcome, is_cross, through_ball, switch
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (event_uuid) DO NOTHING;
                    """, pass_rows, page_size=500)

                # ── fact_shots ──
                shot_events = events[events["type"] == "Shot"] if "type" in events.columns else pd.DataFrame()
                shot_rows = []
                for _, ev in shot_events.iterrows():
                    end_loc = ev.get("shot_end_location")
                    ex = float(end_loc[0]) if isinstance(end_loc, list) and len(end_loc) >= 1 else None
                    ey = float(end_loc[1]) if isinstance(end_loc, list) and len(end_loc) >= 2 else None
                    ez = float(end_loc[2]) if isinstance(end_loc, list) and len(end_loc) >= 3 else None
                    shot_rows.append((
                        str(ev["id"]),
                        float(ev["shot_statsbomb_xg"]) if pd.notna(ev.get("shot_statsbomb_xg")) else None,
                        ex, ey, ez,
                        str(ev.get("shot_outcome", "")) or None,
                        str(ev.get("shot_technique", "")) or None,
                        str(ev.get("shot_body_part", "")) or None,
                        bool(ev.get("shot_first_time", False)),
                        bool(ev.get("shot_one_on_one", False)),
                    ))
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
                log.info("Loaded match %s into PostgreSQL", match_id)

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
