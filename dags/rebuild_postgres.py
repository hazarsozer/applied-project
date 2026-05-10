"""
One-shot recovery script: rebuilds dim and fact tables from raw StatsBomb JSON.

Why this exists: the original DAG used statsbombpy with flatten_attrs=True, which
strips IDs from nested objects (player, team, type). The raw StatsBomb JSON from
GitHub preserves all IDs, so we re-download and parse it directly here.

Run with: docker exec airflow-scheduler python /opt/airflow/dags/rebuild_postgres.py
"""

import json
import os
import urllib.request
import logging

import psycopg2
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("rebuild")

COMPETITIONS = [
    {"competition_id": 55, "season_id": 282,
     "competition_name": "UEFA Euro", "season_name": "2024",
     "country_name": "Europe"},
    {"competition_id": 43, "season_id": 106,
     "competition_name": "FIFA World Cup", "season_name": "2022",
     "country_name": "World"},
]

RAW_BASE = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"


def pg_conn():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )


def truncate_all(cur):
    log.info("Truncating fact and dim tables...")
    cur.execute("""
        TRUNCATE TABLE
            fact_carries, fact_dribbles, fact_shots, fact_passes, fact_events,
            dim_event_types, dim_players, dim_teams, dim_matches, dim_competitions
        RESTART IDENTITY CASCADE;
    """)


def load_competitions_and_matches(cur):
    for comp in COMPETITIONS:
        cur.execute("""
            INSERT INTO dim_competitions
                (competition_id, competition_name, season_id, season_name, country_name)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (competition_id) DO NOTHING;
        """, (comp["competition_id"], comp["competition_name"],
              comp["season_id"], comp["season_name"], comp["country_name"]))

        url = f"{RAW_BASE}/matches/{comp['competition_id']}/{comp['season_id']}.json"
        log.info(f"Fetching matches for {comp['competition_name']}...")
        matches = json.loads(urllib.request.urlopen(url, timeout=30).read())

        for m in matches:
            home_team = m.get("home_team") or {}
            away_team = m.get("away_team") or {}
            stadium = m.get("stadium") or {}
            referee = m.get("referee") or {}

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
        log.info(f"  loaded {len(matches)} matches")


def insert_dims(cur, players, teams, event_types):
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


def process_match(cur, match_id):
    url = f"{RAW_BASE}/events/{match_id}.json"
    try:
        events = json.loads(urllib.request.urlopen(url, timeout=60).read())
    except Exception as exc:
        log.warning(f"Failed to fetch events for match {match_id}: {exc}")
        return 0

    players, teams, event_types = {}, {}, {}
    event_rows, pass_rows, shot_rows = [], [], []

    for ev in events:
        ev_id = ev.get("id")
        if not ev_id:
            continue

        player = ev.get("player") or {}
        team = ev.get("team") or {}
        type_ = ev.get("type") or {}

        player_id = player.get("id") if isinstance(player, dict) else None
        player_name = player.get("name") if isinstance(player, dict) else None
        team_id = team.get("id") if isinstance(team, dict) else None
        team_name = team.get("name") if isinstance(team, dict) else None
        type_id = type_.get("id") if isinstance(type_, dict) else None
        type_name = type_.get("name") if isinstance(type_, dict) else None

        if player_id and player_name:
            players[player_id] = player_name
        if team_id and team_name:
            teams[team_id] = team_name
        if type_id and type_name:
            event_types[type_id] = type_name

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
        if "pass" in ev and isinstance(ev["pass"], dict):
            p = ev["pass"]
            recipient = p.get("recipient") or {}
            recipient_id = recipient.get("id") if isinstance(recipient, dict) else None
            recipient_name = recipient.get("name") if isinstance(recipient, dict) else None
            if recipient_id and recipient_name:
                players[recipient_id] = recipient_name

            end_loc = p.get("end_location") or []
            ex = float(end_loc[0]) if len(end_loc) >= 2 else None
            ey = float(end_loc[1]) if len(end_loc) >= 2 else None

            outcome = p.get("outcome") or {}
            height = p.get("height") or {}
            body_part = p.get("body_part") or {}
            pass_type = p.get("type") or {}

            pass_rows.append((
                ev_id,
                recipient_id,
                p.get("length"),
                p.get("angle"),
                ex, ey,
                height.get("name") if isinstance(height, dict) else None,
                body_part.get("name") if isinstance(body_part, dict) else None,
                pass_type.get("name") if isinstance(pass_type, dict) else None,
                outcome.get("name") if isinstance(outcome, dict) else None,
                bool(p.get("cross", False)),
                bool(p.get("through_ball", False)),
                bool(p.get("switch", False)),
            ))

        # Shot sub-event
        if "shot" in ev and isinstance(ev["shot"], dict):
            s = ev["shot"]
            end_loc = s.get("end_location") or []
            ex = float(end_loc[0]) if len(end_loc) >= 1 else None
            ey = float(end_loc[1]) if len(end_loc) >= 2 else None
            ez = float(end_loc[2]) if len(end_loc) >= 3 else None

            outcome = s.get("outcome") or {}
            technique = s.get("technique") or {}
            body_part = s.get("body_part") or {}

            shot_rows.append((
                ev_id,
                s.get("statsbomb_xg"),
                ex, ey, ez,
                outcome.get("name") if isinstance(outcome, dict) else None,
                technique.get("name") if isinstance(technique, dict) else None,
                body_part.get("name") if isinstance(body_part, dict) else None,
                bool(s.get("first_time", False)),
                bool(s.get("one_on_one", False)),
            ))

    # Insert dims first (FK targets), then facts
    insert_dims(cur, players, teams, event_types)

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

    return len(event_rows)


def refresh_views(cur):
    log.info("Refreshing materialized views...")
    cur.execute("REFRESH MATERIALIZED VIEW mv_match_summary;")
    cur.execute("REFRESH MATERIALIZED VIEW mv_player_stats;")


def main():
    conn = pg_conn()
    cur = conn.cursor()

    truncate_all(cur)
    conn.commit()

    load_competitions_and_matches(cur)
    conn.commit()

    cur.execute("SELECT match_id FROM dim_matches ORDER BY match_id;")
    match_ids = [row[0] for row in cur.fetchall()]
    log.info(f"Found {len(match_ids)} matches to load.")

    total_events = 0
    for i, match_id in enumerate(match_ids, 1):
        n = process_match(cur, match_id)
        total_events += n
        if i % 5 == 0 or i == len(match_ids):
            log.info(f"[{i}/{len(match_ids)}] match {match_id}: +{n} events (total {total_events})")
            conn.commit()

    conn.commit()
    refresh_views(cur)
    conn.commit()

    cur.close()
    conn.close()
    log.info(f"DONE. Total events: {total_events}")


if __name__ == "__main__":
    main()
