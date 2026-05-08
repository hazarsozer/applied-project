# Sample Data

This directory is a placeholder for a small sample StatsBomb event file used for
offline testing or demonstration without internet access.

Full data is downloaded at runtime by the Airflow DAG from the StatsBomb open-data
GitHub repository:
  https://github.com/statsbomb/open-data

Competitions ingested:
  - UEFA Euro 2024  (competition_id=55, season_id=282)
  - La Liga 2015/16 (competition_id=11, season_id=27)

The raw event files are written to the `shared-data` Docker volume at `/shared/raw/`
and processed by Apache NiFi into Elasticsearch.
