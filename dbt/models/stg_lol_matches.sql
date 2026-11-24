with source_matches as (
    select *
    from {{ source('lol_raw', 'raw_lol_matches') }}
)

select
    match_id,
    champion as champion_name,
    queue_id,
    kills,
    deaths,
    assists,
    win as is_win,
    to_timestamp(game_creation_ms / 1000.0) at time zone 'UTC' as game_played_at_utc,
    loaded_at
from source_matches