-- ============================================================
-- Football Pipeline — PostgreSQL Schema
-- Star schema: fact_events + dimension tables
-- ============================================================

-- Airflow uses the default DB (football_db via POSTGRES_DB).
-- We create a separate logical namespace with schemas.
CREATE DATABASE airflow_db;

\connect football_db;

-- ── Dimension: Competitions ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_competitions (
    competition_id   INTEGER PRIMARY KEY,
    competition_name TEXT    NOT NULL,
    season_id        INTEGER NOT NULL,
    season_name      TEXT    NOT NULL,
    country_name     TEXT
);

-- ── Dimension: Matches ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_matches (
    match_id         INTEGER PRIMARY KEY,
    competition_id   INTEGER REFERENCES dim_competitions(competition_id),
    match_date       DATE,
    home_team_id     INTEGER,
    home_team_name   TEXT,
    away_team_id     INTEGER,
    away_team_name   TEXT,
    home_score       SMALLINT,
    away_score       SMALLINT,
    stadium_name     TEXT,
    referee_name     TEXT
);

-- ── Dimension: Players ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_players (
    player_id   INTEGER PRIMARY KEY,
    player_name TEXT NOT NULL
);

-- ── Dimension: Teams ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_teams (
    team_id   INTEGER PRIMARY KEY,
    team_name TEXT NOT NULL
);

-- ── Dimension: Event Types ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_event_types (
    type_id   INTEGER PRIMARY KEY,
    type_name TEXT NOT NULL
);

-- ── Fact: Events (core fact table) ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_events (
    event_uuid   UUID        PRIMARY KEY,
    event_index  INTEGER     NOT NULL,
    match_id     INTEGER     REFERENCES dim_matches(match_id),
    team_id      INTEGER     REFERENCES dim_teams(team_id),
    player_id    INTEGER     REFERENCES dim_players(player_id),
    type_id      INTEGER     REFERENCES dim_event_types(type_id),
    period       SMALLINT    NOT NULL,
    minute       SMALLINT    NOT NULL,
    second       SMALLINT    NOT NULL,
    timestamp    TEXT,
    location_x   NUMERIC(6,2),
    location_y   NUMERIC(6,2),
    duration     NUMERIC(8,3),
    possession   INTEGER,
    under_pressure BOOLEAN   DEFAULT FALSE,
    UNIQUE (match_id, event_index)
);

-- ── Fact: Passes ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_passes (
    event_uuid          UUID PRIMARY KEY REFERENCES fact_events(event_uuid),
    recipient_id        INTEGER REFERENCES dim_players(player_id),
    pass_length         NUMERIC(7,3),
    pass_angle          NUMERIC(7,4),
    pass_end_x          NUMERIC(6,2),
    pass_end_y          NUMERIC(6,2),
    pass_height         TEXT,
    pass_body_part      TEXT,
    pass_type           TEXT,
    pass_outcome        TEXT,
    cross               BOOLEAN DEFAULT FALSE,
    through_ball        BOOLEAN DEFAULT FALSE,
    switch              BOOLEAN DEFAULT FALSE
);

-- ── Fact: Shots ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_shots (
    event_uuid      UUID PRIMARY KEY REFERENCES fact_events(event_uuid),
    shot_xg         NUMERIC(7,5),
    shot_end_x      NUMERIC(6,2),
    shot_end_y      NUMERIC(6,2),
    shot_end_z      NUMERIC(6,2),
    shot_outcome    TEXT,
    shot_technique  TEXT,
    shot_body_part  TEXT,
    first_time      BOOLEAN DEFAULT FALSE,
    one_on_one      BOOLEAN DEFAULT FALSE
);

-- ── Fact: Dribbles ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_dribbles (
    event_uuid       UUID PRIMARY KEY REFERENCES fact_events(event_uuid),
    dribble_outcome  TEXT,
    nutmeg           BOOLEAN DEFAULT FALSE,
    overrun          BOOLEAN DEFAULT FALSE
);

-- ── Fact: Carries ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_carries (
    event_uuid   UUID PRIMARY KEY REFERENCES fact_events(event_uuid),
    carry_end_x  NUMERIC(6,2),
    carry_end_y  NUMERIC(6,2)
);

-- ── Materialised views for analytics ─────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_match_summary AS
SELECT
    m.match_id,
    c.competition_name,
    c.season_name,
    m.match_date,
    m.home_team_name,
    m.away_team_name,
    m.home_score,
    m.away_score,
    COUNT(DISTINCT e.event_uuid)                         AS total_events,
    COUNT(DISTINCT s.event_uuid)                         AS total_shots,
    ROUND(SUM(s.shot_xg)::NUMERIC, 3)                   AS total_xg,
    COUNT(DISTINCT p.event_uuid)                         AS total_passes
FROM dim_matches m
JOIN dim_competitions c   USING (competition_id)
LEFT JOIN fact_events e   USING (match_id)
LEFT JOIN fact_shots  s   ON e.event_uuid = s.event_uuid
LEFT JOIN fact_passes p   ON e.event_uuid = p.event_uuid
GROUP BY m.match_id, c.competition_name, c.season_name,
         m.match_date, m.home_team_name, m.away_team_name,
         m.home_score, m.away_score;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_player_stats AS
SELECT
    pl.player_id,
    pl.player_name,
    t.team_name,
    COUNT(DISTINCT e.match_id)                           AS matches_played,
    COUNT(DISTINCT p.event_uuid)                         AS total_passes,
    COUNT(CASE WHEN p.pass_outcome IS NULL THEN 1 END)  AS successful_passes,
    COUNT(DISTINCT s.event_uuid)                         AS total_shots,
    COUNT(CASE WHEN s.shot_outcome = 'Goal' THEN 1 END) AS goals,
    ROUND(SUM(s.shot_xg)::NUMERIC, 3)                   AS total_xg
FROM dim_players pl
JOIN fact_events e  ON pl.player_id = e.player_id
JOIN dim_teams   t  ON e.team_id    = t.team_id
LEFT JOIN fact_passes p ON e.event_uuid = p.event_uuid
LEFT JOIN fact_shots  s ON e.event_uuid = s.event_uuid
GROUP BY pl.player_id, pl.player_name, t.team_name;

-- ── Indexes ───────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_events_match     ON fact_events (match_id);
CREATE INDEX IF NOT EXISTS idx_events_player    ON fact_events (player_id);
CREATE INDEX IF NOT EXISTS idx_events_team      ON fact_events (team_id);
CREATE INDEX IF NOT EXISTS idx_events_type      ON fact_events (type_id);
CREATE INDEX IF NOT EXISTS idx_events_period    ON fact_events (period, minute);
CREATE INDEX IF NOT EXISTS idx_shots_outcome    ON fact_shots  (shot_outcome);
CREATE INDEX IF NOT EXISTS idx_passes_outcome   ON fact_passes (pass_outcome);
