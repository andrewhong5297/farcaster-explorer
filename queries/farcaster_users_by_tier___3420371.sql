-- part of a query repo
-- query name: Farcaster Users by Tier
-- query link: https://dune.com/queries/3420371


with 
    last_week as (SELECT fid_active_tier_last, count(*) as users_last FROM query_3418402 GROUP BY 1)
    , this_week as (SELECT fid_active_tier_name, fid_active_tier, count(*) as users FROM query_3418402 GROUP BY 1,2)

SELECT
    fid_active_tier_name
    , fid_active_tier
    , users
    , users - COALESCE(users_last,0) as wow_users
FROM this_week tw
LEFT JOIN last_week lw ON tw.fid_active_tier = lw.fid_active_tier_last
order by fid_active_tier desc