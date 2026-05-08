# Football Event Analytics Pipeline

> YZV 322E — Applied Data Engineering · Spring 2026 · Istanbul Technical University

An end-to-end, fully containerised data engineering pipeline that ingests
[StatsBomb open event data](https://github.com/statsbomb/open-data) (UEFA Euro 2024 +
La Liga 2015/16, ~85 matches, >250 000 events), normalises it into a PostgreSQL star
schema, indexes it into Elasticsearch, and exposes it through interactive Kibana
dashboards with a live "match replay" streaming service.

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
  │   Apache NiFi      │  │    PostgreSQL 15     │
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
  ┌─────────────────────┐   ┌────────────────────────┐
  │      Kibana 8.x     │   │   Match Replay Service  │
  │  · Shot map (Vega)  │   │   PostgreSQL → streams  │
  │  · xG trend (Vega)  │◄──│   events into ES at     │
  │  · Pass network     │   │   30× real-time speed   │
  │  · Live replay gauge│   └────────────────────────┘
  └─────────────────────┘
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
git clone <repo-url>
cd applied-proje

# 2. Configure environment
cp .env.example .env
# (default values work out of the box)

# 3. Launch everything
docker compose up --build
```

The entire stack starts with that single command. Cold-start time is approximately
10–15 minutes (NiFi startup is the longest step at ~2 min; data download follows).

To trigger the Airflow pipeline after services are up:

```bash
# Option A — via the Airflow UI (recommended for demo)
# Open http://localhost:8082, log in (admin/admin), enable and trigger the DAG.

# Option B — CLI
docker exec airflow-scheduler airflow dags trigger statsbomb_football_pipeline
```

---

## Service Endpoints

| Service | URL | Credentials |
|---|---|---|
| Airflow Webserver | http://localhost:8082 | admin / admin |
| NiFi UI | http://localhost:8181/nifi | nifi_admin / nifi_admin12345 |
| Kibana | http://localhost:5601 | — (no auth) |
| pgAdmin | http://localhost:5050 | admin@football.local / pgadmin_pass |
| Elasticsearch | http://localhost:9200 | — (no auth) |

---

## Repository Structure

```
.
├── docker-compose.yml          # Single-command stack definition
├── .env.example                # Environment variable template
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
├── kibana/
│   └── saved_objects.ndjson    # Index patterns + Vega-Lite dashboards
├── data/
│   └── sample/                 # Placeholder; full data downloaded at runtime
├── docs/                       # Architecture diagrams (report figures)
└── report/                     # LaTeX technical report
```

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
   - Loops continuously so Kibana's "Live Replay" gauge always shows activity
     during the demo without misrepresenting the batch architecture.

---

## Kibana Dashboards

Navigate to **http://localhost:5601 → Dashboards → Football Event Analytics**.

| Panel | Description |
|---|---|
| Shot Map (Vega-Lite) | Shots plotted on a pitch grid, sized by xG, coloured by outcome |
| xG Trend | Cumulative expected goals per 5-minute bin, by team |
| Pass Network | Pass-density heatmap across pitch zones |
| Events by Type | Bar chart of top 15 event type frequencies |
| Live Replay Gauge | Rolling count of events from the match-replay stream |

---

## PostgreSQL — Useful Queries

```sql
-- Top scorers
\i /docker-entrypoint-initdb.d/../queries/top_scorers.sql

-- Pass accuracy by team per match
\i /docker-entrypoint-initdb.d/../queries/pass_accuracy.sql

-- Match summary materialised view
SELECT * FROM mv_match_summary ORDER BY total_xg DESC LIMIT 10;

-- Player stats
SELECT * FROM mv_player_stats WHERE goals > 3 ORDER BY goals DESC;
```

---

## Known Limitations

- NiFi startup takes ~90–120 seconds; the `nifi-setup` container waits
  automatically via health-check before building the flow.
- Elasticsearch Vega visualisations require the data to be loaded first. If
  dashboards appear empty, trigger the Airflow DAG and wait ~5 minutes.
- The Jolt transform does not handle every event sub-type (e.g., `50_50`,
  `ball_receipt`) — those fields are passed through but not explicitly mapped.
- For machines with < 16 GB RAM, reduce `NIFI_JVM_HEAP_MAX` and
  `ES_JAVA_OPTS` in `.env` to lower values (e.g., 512 MB each).
- StatsBomb data download requires an active internet connection.

---

## Stopping and Cleaning Up

```bash
# Stop containers (data volumes preserved)
docker compose down

# Full reset including volumes
docker compose down -v
```
