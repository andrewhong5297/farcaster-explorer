-- part of a query repo
-- query name: Channels by Tier
-- query link: https://dune.com/queries/3421219


with 
    last_week as (SELECT channel_tier_last, count(*) as channels_last FROM query_3418331 GROUP BY 1)
    , this_week as (SELECT channel_tier_name, channel_tier, count(*) as channels FROM query_3418331 GROUP BY 1,2)

SELECT
    channel_tier_name
    , channel_tier
    , channels
    , channels - COALESCE(channels_last,0) as wow_channels
FROM this_week tw
LEFT JOIN last_week lw ON tw.channel_tier = lw.channel_tier_last
order by channel_tier desc