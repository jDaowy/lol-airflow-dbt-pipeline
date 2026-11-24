with matches as (
    select *
    from {{ ref('stg_lol_matches') }}
)

select
    champion_name,
    count(*) as games_played,
    count(*) filter (where is_win) as wins,
    count(*) filter (where not is_win) as losses,
    round(
        100.0 * count(*) filter (where is_win)::numeric / nullif(count(*), 0),
        1
    ) as win_rate_pct,
    round(avg(kills)::numeric, 1) as avg_kills,
    round(avg(deaths)::numeric, 1) as avg_deaths,
    round(avg(assists)::numeric, 1) as avg_assists,
    round(
        (sum(kills) + sum(assists))::numeric / nullif(sum(deaths), 0),
        2
    ) as kda_ratio
from matches
group by champion_name
order by games_played desc, win_rate_pct desc