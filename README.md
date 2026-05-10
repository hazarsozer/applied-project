# Football Event Analytics Pipeline

> YZV 322E — Applied Data Engineering · Spring 2026 · Istanbul Technical University

An end-to-end, fully containerised data engineering pipeline that ingests
[StatsBomb open event data](https://github.com/statsbomb/open-data)
(**UEFA Euro 2024** + **FIFA World Cup 2022** — 115 matches, ~240 000 events),
normalises it into a PostgreSQL star schema, indexes it into Elasticsearch,
and exposes it through interactive Kibana dashboards with a live "match replay"
streaming service.

## Team

| Name | Student ID | Email |
|---|---|---|
| Mustafa İhsan Yüce | 150210333 | yucem21@itu.edu.tr |
| Hazar Utku Sozer | 150220754 | sozer20@itu.edu.tr |
| Hüseyin Korkut | 150210314 | korkuth21@itu.edu.tr |
| Faruk Çevik | 150220325 | cevikf22@itu.edu.tr |

---

## Architecture

```
StatsBomb GitHub (open JSON)
        │
        ▼
  ┌─────────────────────────────────────────┐
  │  Apache Airflow  (orchestration)        │
  │  · download_raw_events  → /shared/raw/ │
  │  · load_dimensions      → PostgreSQL   │
  │  · load_fact_tables     → PostgreSQL   │
  │  · refresh_views        → PostgreSQL   │
  │  · trigger_nifi         (signal)       │
  │  · verify_elasticsearch (health check) │
  └───────────┬─────────────────┬───────────┘
              │ /shared/raw/    │ psycopg2
              ▼                 ▼
  ┌────────────────────┐  ┌─────────────────────┐
  │   Apache NiFi      │  │    PostgreSQL 16     │
  │   GetFile          │  │    star schema       │
  │   → SplitJson      │  │    · dim_matches     │
  │   → JoltTransform  │  │    · dim_players     │
  │   → ReplaceText    │  │    · fact_events     │
  │   → MergeContent   │  │    · fact_passes     │
  │   → InvokeHTTP     │  │    · fact_shots      │
  └────────┬───────────┘  └──────────┬──────────┘
           │ /_bulk NDJSON            │ pgAdmin
           ▼                         ▼
  ┌─────────────────────┐  ┌─────────────────────┐
  │  Elasticsearch 8.x  │  │      pgAdmin 4       │
  │  football_events    │  │  port 5050           │
  │  football_replay    │  └─────────────────────┘
  └────────┬────────────┘
           │
           ▼
  ┌──────────────────────────────┐   ┌────────────────────────┐
  │         Kibana 8.x           │   │   Match Replay Service  │
  │  Dashboard 1: UEFA Euro 2024 │   │   PostgreSQL → streams  │
  │  Dashboard 2: World Cup 2022 │◄──│   events into ES at     │
  │  Dashboard 3: Live Replay    │   │   30× real-time speed   │
  └──────────────────────────────┘   └────────────────────────┘
```

**Course tools used:** Apache Airflow · Apache NiFi · PostgreSQL · pgAdmin ·
Elasticsearch · Kibana (all 6 mandatory tools).

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) ≥ 24 (with
  Docker Compose v2)
- 16 GB RAM, ~20 GB free disk
- Internet access (data downloaded from StatsBomb GitHub at startup)

---

## Quick Start

```bash
# 1. Clone
git clone https://github.com/hazarsozer/applied-project.git
cd applied-project

# 2. Configure environment
cp .env.example .env
# (default values work out of the box — edit only if needed)

# 3. Launch the full stack
docker compose up --build -d
```

The `nifi-setup` container runs automatically on first boot and:
- Creates the Elasticsearch index with field mappings
- Imports all Kibana dashboards
- Builds and starts the NiFi processing flow

Once all services are healthy (~3–5 minutes), trigger the data pipeline:

```bash
# Option A — Airflow UI (recommended)
# Open http://localhost:8082, log in (admin / admin),
# find "statsbomb_football_pipeline" and click ▶ Trigger DAG.

# Option B — CLI
docker exec airflow-scheduler airflow dags trigger statsbomb_football_pipeline
```

Full ingestion of all 115 matches takes **20–30 minutes** (download +
NiFi indexing). The Kibana dashboards populate incrementally as events arrive.

---

## Service Endpoints

| Service | URL | Credentials |
|---|---|---|
| Airflow Webserver | http://localhost:8082 | admin / admin |
| NiFi UI | http://localhost:8181/nifi | see `.env` |
| Kibana | http://localhost:5601 | — (no auth) |
| pgAdmin | http://localhost:5050 | see `.env` |
| Elasticsearch | http://localhost:9200 | — (no auth) |

---

## Repository Structure

```
.
├── docker-compose.yml          # Single-command stack definition
├── .env.example                # Environment variable template
├── generate_kibana_objects.py  # Kibana saved-objects generator script
├── dags/
│   ├── Dockerfile              # Custom Airflow image (adds statsbombpy etc.)
│   ├── requirements.txt
│   └── statsbomb_dag.py        # Main ETL DAG (7 tasks, idempotent)
├── sql/
│   ├── init.sql                # Star schema DDL + materialised views
│   └── queries/                # Example analytical SQL
├── src/
│   ├── nifi_setup/
│   │   ├── Dockerfile
│   │   ├── setup_flow.py       # NiFi REST API flow builder + ES/Kibana init
│   │   ├── jolt_spec.json      # Jolt SHIFT spec (flattens nested StatsBomb JSON)
│   │   └── requirements.txt
│   └── match_replay/
│       ├── Dockerfile
│       ├── replay.py           # Streams match events into ES at 30× speed
│       └── requirements.txt
├── elasticsearch/
│   └── events_mapping.json     # ES index mappings with geo_point for location
└── kibana/
    └── saved_objects.ndjson    # Index patterns + all Kibana dashboards
```

---

## Datasets

Both competitions are complete, full-tournament StatsBomb open datasets.
They are kept entirely separate — no cross-competition data is mixed.

| Competition | Season | Matches | Events |
|---|---|---|---|
| UEFA Euro 2024 | 2024 | 51 | ~113 000 |
| FIFA World Cup 2022 | 2022 | 64 | ~127 000 |

---

## Data Flow Detail

1. **Airflow DAG** (`statsbomb_football_pipeline`, `@once`):
   - Downloads raw event JSON from `github.com/statsbomb/open-data` into
     `/shared/raw/{match_id}.json` (idempotent — skips existing files).
   - Loads dimension tables (`dim_competitions`, `dim_matches`, `dim_teams`,
     `dim_players`) directly into PostgreSQL via `psycopg2`.
   - Loads fact tables (`fact_events`, `fact_passes`, `fact_shots`) using
     `statsbombpy` DataFrames for type-safe column access.
   - Refreshes materialised views (`mv_match_summary`, `mv_player_stats`).

2. **NiFi Flow** (auto-configured by `nifi-setup` container at startup):
   - `GetFile` watches `/shared/raw/` for files written by Airflow.
   - `SplitJson` explodes each match file's event array into individual FlowFiles.
   - `JoltTransformJSON` applies the Jolt SHIFT spec to flatten nested fields
     (e.g., `pass.end_location[0]` → `pass_end_x`, `shot.statsbomb_xg` → `shot_xg`).
   - `ReplaceText` prepends an Elasticsearch bulk-action header to each event.
   - `MergeContent` batches 100 events into one NDJSON payload.
   - `InvokeHTTP` POSTs to `http://elasticsearch:9200/_bulk`.

3. **Match Replay Service**:
   - Reads all events for the most event-rich match from PostgreSQL.
   - Streams them into the `football_replay` Elasticsearch index at 30× real speed.
   - Loops continuously so Kibana's Live Replay dashboard always shows activity.

---

## Kibana Dashboards

Open **http://localhost:5601 → Dashboards** after the pipeline has run.

### Dashboard 1 — UEFA Euro 2024 Analytics
### Dashboard 2 — FIFA World Cup 2022 Analytics

Each dashboard contains the same set of panels, filtered strictly to its own
competition (no data mixing):

| Panel | Type | Description |
|---|---|---|
| 5 KPI tiles | Metric | Matches played, Goals, Shots, Total xG, Passes |
| Top 15 Goalscorers | Vega-Lite bar | Players ranked by goals, coloured by team |
| xG vs Goals Scatter | Vega-Lite scatter | Points above y=x line = over-performing xG |
| Top Teams by xG | Vega-Lite bar | Teams ranked by expected goals generated |
| Goals by Match Minute | Vega-Lite area | 5-min bins, stacked by team |
| Shot Distribution Heatmap | Vega-Lite rect | Pitch overlay — shot density by zone |
| Interactive Shot Map | Vega-Lite circles | Shots on pitch, sized by xG; use filter bar to select a team |
| Player Performance Table | Data table | Top 20 players by xG with shot count |

### Dashboard 3 — Live Replay
Metric tile showing rolling event count from the match-replay streaming service.
Set the Kibana time picker to **Last 5 minutes** to see live activity.

---

## PostgreSQL — Useful Queries

```sql
-- Match summary
SELECT * FROM mv_match_summary ORDER BY total_xg DESC LIMIT 10;

-- Player stats
SELECT * FROM mv_player_stats WHERE goals > 3 ORDER BY goals DESC;

-- Top scorers per competition
SELECT p.player_name, t.team_name, COUNT(*) AS goals
FROM fact_events e
JOIN fact_shots s ON e.event_uuid = s.event_uuid
JOIN dim_players p ON e.player_id = p.player_id
JOIN dim_teams t ON e.team_id = t.team_id
JOIN dim_matches m ON e.match_id = m.match_id
JOIN dim_competitions c ON m.competition_id = c.competition_id
WHERE s.shot_outcome = 'Goal'
GROUP BY p.player_name, t.team_name, c.competition_name
ORDER BY goals DESC
LIMIT 20;
```

---

## Known Limitations

- NiFi startup takes ~90–120 seconds; the `nifi-setup` container waits
  automatically via health-check before building the flow.
- Elasticsearch Vega visualisations require the pipeline to have run first.
  If dashboards appear empty, trigger the Airflow DAG and wait ~30 minutes.
- The Jolt transform does not handle every event sub-type (e.g., `50_50`,
  `ball_receipt`) — those fields are passed through but not explicitly mapped.
- For machines with < 16 GB RAM, reduce `NIFI_JVM_HEAP_MAX` and
  `ES_JAVA_OPTS` in `.env` (e.g., 512 MB each).
- StatsBomb data download requires an active internet connection.

---

## Stopping and Cleaning Up

```bash
# Stop containers (data volumes preserved)
docker compose down

# Full reset including all volumes (re-ingestion required on next start)
docker compose down -v
```
