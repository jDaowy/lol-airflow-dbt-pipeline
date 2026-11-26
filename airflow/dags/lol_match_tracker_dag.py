import os
import time
from datetime import datetime
import psycopg2
import requests
from airflow.sdk import dag, task


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lol", "tracker", "riot-api", "dbt"],
)
def lol_match_tracker_dag():
    @task
    def fetch_recent_matches() -> list[dict]:
        api_key = os.environ["RIOT_API_KEY"]
        headers = {"X-Riot-Token": api_key}
        routing = "https://americas.api.riotgames.com"

        game_name = os.environ.get("RIOT_GAME_NAME", "Tyler12345BB")
        tag_line = os.environ.get("RIOT_TAG_LINE", "na1")
        match_count = int(os.environ.get("RIOT_MATCH_COUNT", "20"))

        acct_resp = requests.get(
            f"{routing}/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}",
            headers=headers,
            timeout=15,
        )
        acct_resp.raise_for_status()
        puuid = acct_resp.json()["puuid"]

        ids_resp = requests.get(
            f"{routing}/lol/match/v5/matches/by-puuid/{puuid}/ids",
            headers=headers,
            params={"count": match_count},
            timeout=15,
        )
        ids_resp.raise_for_status()
        match_ids = ids_resp.json()

        matches: list[dict] = []
        for idx, match_id in enumerate(match_ids):
            if idx > 0:
                time.sleep(1.2)

            match_resp = requests.get(
                f"{routing}/lol/match/v5/matches/{match_id}",
                headers=headers,
                timeout=15,
            )
            match_resp.raise_for_status()
            match_json = match_resp.json()

            info = match_json.get("info", {})
            participants = info.get("participants", [])
            player_stats = next(
                (p for p in participants if p.get("puuid") == puuid), None
            )
            if not player_stats:
                continue

            matches.append(
                {
                    "match_id": match_id,
                    "game_creation_ms": info.get("gameCreation"),
                    "queue_id": info.get("queueId"),
                    "champion": player_stats["championName"],
                    "kills": player_stats["kills"],
                    "deaths": player_stats["deaths"],
                    "assists": player_stats["assists"],
                    "win": bool(player_stats["win"]),
                }
            )

        return matches

    @task
    def load_matches_to_postgres(matches: list[dict]) -> int:
        pg_user = os.environ.get("POSTGRES_USER", "airflow")
        pg_password = os.environ.get("POSTGRES_PASSWORD", "airflow")

        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            dbname="lol_analytics",
            user=pg_user,
            password=pg_password,
        )
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS public.raw_lol_matches (
                            match_id TEXT PRIMARY KEY,
                            game_creation_ms BIGINT,
                            queue_id INTEGER,
                            champion TEXT NOT NULL,
                            kills INTEGER NOT NULL,
                            deaths INTEGER NOT NULL,
                            assists INTEGER NOT NULL,
                            win BOOLEAN NOT NULL,
                            loaded_at TIMESTAMPTZ DEFAULT NOW()
                        );
                        """
                    )

                    upsert_sql = """
                        INSERT INTO public.raw_lol_matches (
                            match_id,
                            game_creation_ms,
                            queue_id,
                            champion,
                            kills,
                            deaths,
                            assists,
                            win,
                            loaded_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (match_id) DO UPDATE SET
                            game_creation_ms = EXCLUDED.game_creation_ms,
                            queue_id = EXCLUDED.queue_id,
                            champion = EXCLUDED.champion,
                            kills = EXCLUDED.kills,
                            deaths = EXCLUDED.deaths,
                            assists = EXCLUDED.assists,
                            win = EXCLUDED.win,
                            loaded_at = NOW();
                        """

                    rows = [
                        (
                            m["match_id"],
                            m.get("game_creation_ms"),
                            m.get("queue_id"),
                            m["champion"],
                            m["kills"],
                            m["deaths"],
                            m["assists"],
                            m["win"],
                        )
                        for m in matches
                    ]
                    cur.executemany(upsert_sql, rows)
            return len(rows)
        finally:
            conn.close()

    recent_matches = fetch_recent_matches()
    load_matches_to_postgres(recent_matches)


lol_match_tracker_dag()