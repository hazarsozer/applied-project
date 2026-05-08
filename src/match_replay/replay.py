"""
Match Replay Service
Streams a single football match's events into a dedicated Elasticsearch index
at accelerated wall-clock speed, demonstrating live ingestion behavior.

The service:
  1. Waits for PostgreSQL and Elasticsearch to be ready.
  2. Waits until at least one match is loaded into PostgreSQL.
  3. Selects the match with the most events for an engaging demo.
  4. Replays events at REPLAY_SPEED× real-time into the `football_replay` index.
  5. After completion, loops through the same match indefinitely (so the
     Kibana dashboard always has live-looking data during the presentation).

This service intentionally does NOT misrepresent the underlying batch
architecture — it is framed as a demonstration tool in the technical report.
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta

import psycopg2
import requests
from elasticsearch import Elasticsearch, helpers

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [REPLAY] %(levelname)s %(message)s",
)
log = logging.getLogger("match-replay")

# ── Config ────────────────────────────────────────────────────────────────────
PG_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST",     "postgres"),
    "port":     int(os.environ.get("POSTGRES_PORT", "5432")),
    "user":     os.environ.get("POSTGRES_USER",     "football_user"),
    "password": os.environ.get("POSTGRES_PASSWORD", "football_pass"),
    "dbname":   os.environ.get("POSTGRES_DB",       "football_db"),
}
ES_HOST        = os.environ.get("ES_HOST",         "elasticsearch")
ES_PORT        = int(os.environ.get("ES_PORT",     "9200"))
ES_REPLAY_IDX  = os.environ.get("ES_REPLAY_INDEX", "football_replay")
REPLAY_SPEED   = float(os.environ.get("REPLAY_SPEED", "30"))   # 30× faster than real-time
BATCH_SIZE     = 20                                              # events sent per ES bulk call


# ── Readiness helpers ─────────────────────────────────────────────────────────

def _wait_for_postgres(retries: int = 60, delay: int = 10) -> psycopg2.extensions.connection:
    for i in range(retries):
        try:
            conn = psycopg2.connect(**PG_CONFIG)
            log.info("PostgreSQL ready.")
            return conn
        except Exception as exc:
            log.debug("PG not ready (attempt %d): %s", i + 1, exc)
            time.sleep(delay)
    raise RuntimeError("PostgreSQL did not become ready in time.")


def _wait_for_elasticsearch(retries: int = 60, delay: int = 10) -> Elasticsearch:
    es = Elasticsearch(f"http://{ES_HOST}:{ES_PORT}")
    for i in range(retries):
        try:
            if es.ping():
                log.info("Elasticsearch ready.")
                return es
        except Exception as exc:
            log.debug("ES not ready (attempt %d): %s", i + 1, exc)
        time.sleep(delay)
    raise RuntimeError("Elasticsearch did not become ready in time.")


def _wait_for_data(conn, retries: int = 60, delay: int = 15) -> int:
    """Block until at least one match has been loaded into PostgreSQL."""
    for i in range(retries):
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM fact_events;")
        count = cur.fetchone()[0]
        cur.close()
        if count > 0:
            log.info("PostgreSQL has %d events — ready to replay.", count)
            return count
        log.info("Waiting for data… (attempt %d, 0 events so far)", i + 1)
        time.sleep(delay)
    raise RuntimeError("No data appeared in PostgreSQL within the wait window.")


# ── Replay logic ──────────────────────────────────────────────────────────────

def _pick_match(conn) -> dict:
    """Choose the match with the most events (most engaging for demo)."""
    cur = conn.cursor()
    cur.execute("""
        SELECT
            m.match_id,
            m.home_team_name,
            m.away_team_name,
            m.home_score,
            m.away_score,
            c.competition_name,
            c.season_name,
            COUNT(e.event_uuid) AS event_count
        FROM dim_matches m
        JOIN dim_competitions c USING (competition_id)
        JOIN fact_events      e USING (match_id)
        GROUP BY m.match_id, m.home_team_name, m.away_team_name,
                 m.home_score, m.away_score, c.competition_name, c.season_name
        ORDER BY event_count DESC
        LIMIT 1;
    """)
    row = cur.fetchone()
    cur.close()
    if not row:
        raise RuntimeError("No matches found in dim_matches.")
    return {
        "match_id":         row[0],
        "home_team":        row[1],
        "away_team":        row[2],
        "home_score":       row[3],
        "away_score":       row[4],
        "competition":      row[5],
        "season":           row[6],
        "event_count":      row[7],
    }


def _load_match_events(conn, match_id: int) -> list[dict]:
    """Load all events for a match with full dimension data."""
    cur = conn.cursor()
    cur.execute("""
        SELECT
            e.event_uuid,
            e.event_index,
            e.period,
            e.minute,
            e.second,
            e.timestamp,
            e.location_x,
            e.location_y,
            e.duration,
            e.under_pressure,
            t.team_name,
            p.player_name,
            et.type_name,
            -- pass details
            s_pass.pass_length,
            s_pass.pass_angle,
            s_pass.pass_end_x,
            s_pass.pass_end_y,
            s_pass.pass_outcome,
            s_pass.pass_body_part,
            s_pass.cross,
            -- shot details
            s_shot.shot_xg,
            s_shot.shot_end_x,
            s_shot.shot_end_y,
            s_shot.shot_outcome,
            s_shot.shot_technique,
            s_shot.shot_body_part,
            s_shot.first_time
        FROM fact_events e
        LEFT JOIN dim_teams      t  ON e.team_id   = t.team_id
        LEFT JOIN dim_players    p  ON e.player_id = p.player_id
        LEFT JOIN dim_event_types et ON e.type_id  = et.type_id
        LEFT JOIN fact_passes    s_pass ON e.event_uuid = s_pass.event_uuid
        LEFT JOIN fact_shots     s_shot ON e.event_uuid = s_shot.event_uuid
        WHERE e.match_id = %s
        ORDER BY e.event_index ASC;
    """, (match_id,))
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()
    return rows


def _ensure_replay_index(es: Elasticsearch) -> None:
    if not es.indices.exists(index=ES_REPLAY_IDX):
        es.indices.create(index=ES_REPLAY_IDX, body={
            "settings": {"number_of_shards": 1, "number_of_replicas": 0},
            "mappings": {
                "properties": {
                    "replay_ts":   {"type": "date"},
                    "match_id":    {"type": "integer"},
                    "period":      {"type": "integer"},
                    "minute":      {"type": "integer"},
                    "second":      {"type": "integer"},
                    "team_name":   {"type": "keyword"},
                    "player_name": {"type": "keyword"},
                    "type_name":   {"type": "keyword"},
                    "location_x":  {"type": "float"},
                    "location_y":  {"type": "float"},
                    "shot_xg":     {"type": "float"},
                    "shot_outcome":{"type": "keyword"},
                    "pass_outcome":{"type": "keyword"},
                }
            }
        })
        log.info("Created Elasticsearch index '%s'", ES_REPLAY_IDX)


def _replay_match(es: Elasticsearch, match: dict, events: list[dict], loop_n: int) -> None:
    label = f"{match['home_team']} {match['home_score']}–{match['away_score']} {match['away_team']}"
    log.info("[Loop %d] Replaying: %s (%s %s) — %d events @ %.0fx speed",
             loop_n, label, match["competition"], match["season"],
             len(events), REPLAY_SPEED)

    base_ts = datetime.utcnow()
    batch: list[dict] = []

    for i, ev in enumerate(events):
        # Compute accelerated real-time delay between consecutive events
        if i > 0:
            prev = events[i - 1]
            real_gap_seconds = (
                ev["minute"] * 60 + ev["second"]
                - prev["minute"] * 60 - prev["second"]
            )
            # Clamp to avoid sleeping on half-time breaks (> 15 real minutes)
            gap = max(0.0, min(real_gap_seconds, 900)) / REPLAY_SPEED
            if gap > 0:
                time.sleep(gap)

        doc = {
            "_index":      ES_REPLAY_IDX,
            "_id":         f"{loop_n}_{ev['event_uuid']}",
            "_source": {
                "replay_ts":     (base_ts + timedelta(
                    seconds=(ev["minute"] * 60 + ev["second"]) / REPLAY_SPEED
                )).isoformat() + "Z",
                "loop":          loop_n,
                "match_id":      match["match_id"],
                "competition":   match["competition"],
                "season":        match["season"],
                "home_team":     match["home_team"],
                "away_team":     match["away_team"],
                "period":        ev["period"],
                "minute":        ev["minute"],
                "second":        ev["second"],
                "timestamp":     ev["timestamp"],
                "type_name":     ev["type_name"],
                "team_name":     ev["team_name"],
                "player_name":   ev["player_name"],
                "location_x":    float(ev["location_x"])  if ev["location_x"]  is not None else None,
                "location_y":    float(ev["location_y"])  if ev["location_y"]  is not None else None,
                "under_pressure": ev["under_pressure"],
                # pass
                "pass_length":   float(ev["pass_length"]) if ev["pass_length"] is not None else None,
                "pass_angle":    float(ev["pass_angle"])  if ev["pass_angle"]  is not None else None,
                "pass_end_x":    float(ev["pass_end_x"])  if ev["pass_end_x"]  is not None else None,
                "pass_end_y":    float(ev["pass_end_y"])  if ev["pass_end_y"]  is not None else None,
                "pass_outcome":  ev["pass_outcome"],
                "pass_body_part":ev["pass_body_part"],
                "cross":         ev["cross"],
                # shot
                "shot_xg":       float(ev["shot_xg"])     if ev["shot_xg"]     is not None else None,
                "shot_outcome":  ev["shot_outcome"],
                "shot_technique":ev["shot_technique"],
                "shot_body_part":ev["shot_body_part"],
                "shot_first_time": ev["first_time"],
            },
        }
        batch.append(doc)

        if len(batch) >= BATCH_SIZE:
            try:
                helpers.bulk(es, batch, raise_on_error=False)
            except Exception as exc:
                log.warning("Bulk index error: %s", exc)
            batch.clear()

        if (i + 1) % 100 == 0:
            log.info("  [Loop %d] %d/%d events replayed", loop_n, i + 1, len(events))

    if batch:
        try:
            helpers.bulk(es, batch, raise_on_error=False)
        except Exception as exc:
            log.warning("Final batch error: %s", exc)

    log.info("[Loop %d] Replay complete — %d events indexed to '%s'",
             loop_n, len(events), ES_REPLAY_IDX)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    conn = _wait_for_postgres()
    es   = _wait_for_elasticsearch()
    _wait_for_data(conn)
    _ensure_replay_index(es)

    match  = _pick_match(conn)
    events = _load_match_events(conn, match["match_id"])

    loop = 1
    while True:
        _replay_match(es, match, events, loop)
        loop += 1
        log.info("Waiting 30s before next loop…")
        time.sleep(30)


if __name__ == "__main__":
    main()
