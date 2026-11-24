# League of Legends Match & Champion Analytics Pipeline

An end-to-end ELT pipeline built with **Apache Airflow**, **PostgreSQL**, and **dbt** to ingest match history from the Riot Games API, store raw match records, and transform them into clean staging and champion performance models with automated data quality tests.

## Architecture

1. **Extract & Load (Airflow):** `lol_match_tracker_dag` fetches recent matches from the Riot Games API and loads/upserts them into PostgreSQL (`public.raw_lol_matches`).
2. **Transform (dbt):**
   - `analytics.stg_lol_matches` — cleans and normalizes match-level records.
   - `analytics.fct_champion_winrate` — aggregates games played, win rate, and KDA metrics per champion.
3. **Test (dbt):** Automated schema tests (`unique`, `not_null`) verify key columns and model integrity.

## Project Structure

- `airflow/` — Docker Compose setup and DAG definitions (`dags/lol_match_tracker_dag.py`)
- `dbt/` — `dbt` project, SQL models, and `schema.yml` data tests

## How to Run

1. Start Airflow and PostgreSQL:
   ```bash
   cd airflow
   docker compose up -d