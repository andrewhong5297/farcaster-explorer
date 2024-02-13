-- part of a query repo
-- query name: All Active Farcaster Channels
-- query link: https://dune.com/queries/3418331


with 
    reactions as (
        SELECT 
            target_hash 
            , sum(case when reaction_type = 1 then 1 else 0 end) as got_likes
            , sum(case when reaction_type = 2 then 1 else 0 end) as got_recasts
        FROM dune.neynar.dataset_farcaster_reactions
        WHERE deleted_at is null
        AND created_at >= now() - interval '14' day
        AND fid != target_fid
        GROUP BY 1
    )
    
    , cast_stats as (
        with base as (
            --get cast stats by channel, user, and hash
            SELECT 
                COALESCE(c.root_parent_url,c.parent_url) as parent_url
                , c.fid
                , c.hash
                , c.root_parent_hash
                , c.created_at
                , r.got_likes as likes
                , r.got_recasts as recasts
                , approx_distinct(re.hash) as replies
            FROM dune.neynar.dataset_farcaster_casts c
            LEFT JOIN reactions r ON r.target_hash = c.hash 
            LEFT JOIN dune.neynar.dataset_farcaster_casts re ON re.root_parent_hash = c.hash 
                AND re.deleted_at is null 
                AND re.fid != c.fid 
                AND re.created_at >= now() - interval '14' day
            WHERE c.deleted_at is null
            and COALESCE(c.parent_url, c.root_parent_url) is not null --has a channel
            and c.created_at >= now() - interval '14' day
            GROUP BY 1,2,3,4,5,6,7
        )
        
        , caster_summary as (
            --now get summaries week over week
            SELECT 
                parent_url
                , b.fid
                , c.fname
                , c.fid_active_tier
                , c.total_transactions
                , c.trading_volume_usd
                , c.contracts_deployed
                , sum(case when created_at between (now() - interval '14' day) and (now() - interval '7' day) then 1 else 0 end) as active_last_week
                , sum(case when created_at >= now() - interval '7' day then 1 else 0 end) as active_this_week
                , sum(case when created_at between (now() - interval '14' day) and (now() - interval '7' day) then b.likes + b.recasts*3 + b.replies*10 else 0 end) as engagement_last
                , sum(case when created_at >= now() - interval '7' day then b.likes + b.recasts*3 + b.replies*10 else 0 end) as engagement
                , sum(case when created_at >= now() - interval '7' day and root_parent_hash is null then 1 else 0 end) as casts_in_channel --only keep parent casts
            FROM base b 
            LEFT JOIN query_3418402 c ON b.fid = c.fid
            GROUP BY 1,2,3,4,5,6,7
        )
        
        SELECT 
            parent_url
            , slice(array_agg(fname ORDER BY casts_in_channel desc),1,10) as top_casters --top parent casters
            , slice(array_agg(fname ORDER BY fid_active_tier desc, casts_in_channel desc) filter (WHERE fid_active_tier >= 3),1,10) as influential_casters --tier 3 and 4 casters only
            , sum(case when active_this_week > 0 and fid_active_tier = 0 then 1 else 0 end) as active_npc
            , sum(case when active_this_week > 0 and fid_active_tier = 1 then 1 else 0 end) as active_user
            , sum(case when active_this_week > 0 and fid_active_tier = 2 then 1 else 0 end) as active_star
            , sum(case when active_this_week > 0 and fid_active_tier = 3 then 1 else 0 end) as active_influencer
            , sum(case when active_this_week > 0 and fid_active_tier = 4 then 1 else 0 end) as active_vip
            , sum(case when active_last_week > 0 and fid_active_tier = 0 then 1 else 0 end) as active_npc_last
            , sum(case when active_last_week > 0 and fid_active_tier = 1 then 1 else 0 end) as active_user_last
            , sum(case when active_last_week > 0 and fid_active_tier = 2 then 1 else 0 end) as active_star_last
            , sum(case when active_last_week > 0 and fid_active_tier = 3 then 1 else 0 end) as active_influencer_last
            , sum(case when active_last_week > 0 and fid_active_tier = 4 then 1 else 0 end) as active_vip_last
            , sum(engagement) as engagement
            , sum(engagement_last) as engagement_last
            , avg(total_transactions) filter (WHERE total_transactions > 0) as avg_txs
            , avg(trading_volume_usd) filter (WHERE total_transactions > 0) as avg_volume_usd
            , avg(contracts_deployed) filter (WHERE total_transactions > 0) as avg_contracts_deployed
        FROM caster_summary
        group by 1
    )
    
    , channel_domains as (
        with base as (
            SELECT
                COALESCE(root_parent_url,parent_url) as parent_url
                , replace(split(regexp_extract(text, 'https://\S*'),'/')[3],'www.','') as domain
                , count(*) as ct
            FROM dune.neynar.dataset_farcaster_casts c
            WHERE c.deleted_at is null
            and COALESCE(c.parent_url, c.root_parent_url) is not null --has a channel 
            and c.created_at >= now() - interval '7' day --whole query is 7 day trends
            GROUP BY 1,2
        )
        
        SELECT 
            parent_url
            , filter(slice(array_agg(domain order by ct desc),1,3),x-> x is not null) as top_domains 
        FROM base
        WHERE domain NOT IN ('warpcast.com','x.com','twitter.com')
        group by 1
    )
    
    , all_channels as (
        SELECT
            COALESCE(root_parent_url,parent_url) as parent_url
            , max(date_diff('day',date_trunc('day',c.created_at),date_trunc('day',now()))) as channel_age
            --get this week
            , approx_distinct(case when c.created_at >= now() - interval '7' day and c.parent_hash is null then c.hash else null end) as rolling_7_casts
            , approx_distinct(case when c.created_at >= now() - interval '7' day and c.parent_hash is not null then c.hash else null end) as rolling_7_replies
            , approx_distinct(case when c.created_at >= now() - interval '7' day then c.fid else null end) as rolling_7_casters
            , sum(case when c.created_at >= now() - interval '7' day then r.got_likes else null end) as rolling_7_likes
            , sum(case when c.created_at >= now() - interval '7' day then r.got_recasts else null end) as rolling_7_recasts
            --get the week prior
            , approx_distinct(case when c.created_at between (now() - interval '14' day) and (now() - interval '7' day) and c.parent_hash is null then c.hash else null end) as last_7_casts
            , approx_distinct(case when c.created_at between (now() - interval '14' day) and (now() - interval '7' day) and c.parent_hash is not null then c.hash else null end) as last_7_replies
            , approx_distinct(case when c.created_at between (now() - interval '14' day) and (now() - interval '7' day) then c.fid else null end) as last_7_casters
            , sum(case when c.created_at between (now() - interval '14' day) and (now() - interval '7' day) then r.got_likes else null end) as last_7_likes
            , sum(case when c.created_at between (now() - interval '14' day) and (now() - interval '7' day) then r.got_recasts else null end) as last_7_recasts
            , array_agg(distinct c.fid) as all_caster_fids --for param filters later.
        FROM dune.neynar.dataset_farcaster_casts c
        LEFT JOIN reactions r ON r.target_hash = c.hash 
        WHERE c.deleted_at is null
        and COALESCE(c.parent_url, c.root_parent_url) is not null --has a channel
        GROUP BY 1
    )
    
SELECT 
    case 
        when rolling_7_casts >= 250 and engagement >= 100000 and (active_influencer+active_vip) >= 10 then '👑 stadium' --everyone on the platform sees what goes on here
        when rolling_7_casts >= 100 and engagement >= 25000 and active_star >= 50 and (active_influencer+active_vip) >= 2 then '🎭 subculture' -- probably a semi-large community
        when rolling_7_casts >= 25 and engagement >= 5000 and rolling_7_casters >= 100 then '🔍 niche' --active, probably somewhat niche topic
        when rolling_7_casts >= 5 and engagement >= 50 then '🍻 friends' --baby topic
        else '💤 quiet' --basically dead
    end as channel_tier_name
    , case
        when rolling_7_casts >= 250 and engagement >= 100000 and (active_influencer+active_vip) >= 10 then 4
        when rolling_7_casts >= 100 and engagement >= 25000 and active_star >= 50 and (active_influencer+active_vip) >= 2 then 3
        when rolling_7_casts >= 25 and engagement >= 5000 and rolling_7_casters >= 100 then 2
        when rolling_7_casts >= 5 and engagement >= 50 then 1
        else 0 --basically dead
    end as channel_tier
    , case
        when last_7_casts >= 250 and engagement_last >= 100000 and (active_influencer_last+active_vip_last) >= 10 then 4 
        when last_7_casts >= 100 and engagement_last >= 25000 and active_star_last >= 50 and (active_influencer_last+active_vip_last) >= 2 then 3 
        when last_7_casts >= 25 and engagement_last >= 5000 and last_7_casters >= 100 then 2
        when last_7_casts >= 5 and engagement_last >= 50 then 1
        else 0
    end as channel_tier_last
    , get_href(COALESCE('https://warpcast.com/~/channel/' || COALESCE(purl.channel_id,try(split(ac.parent_url,'/')[6])), ac.parent_url)
        , COALESCE(purl.channel_id,try(split(ac.parent_url,'/')[6]),ac.parent_url)
        ) as channel
    , ac.channel_age
    , slice(st.influential_casters,1,3) as influential_casters
    , rolling_7_casts
    , rolling_7_casts - last_7_casts as wow_cast
    , st.engagement
    , st.engagement - st.engagement_last as wow_engage
    , '||' as split_1
    , st.active_npc
    , st.active_npc - st.active_npc_last as wow_npc
    , st.active_user
    , st.active_user - st.active_user_last as wow_active_user
    , st.active_star
    , st.active_star - st.active_star_last as wow_star
    , st.active_influencer
    , st.active_influencer - st.active_influencer_last as wow_influencer
    , st.active_vip
    , st.active_vip - st.active_vip_last as wow_vip
    , '||' as split_0
    , rolling_7_replies
    , rolling_7_replies - last_7_replies as wow_reply
    , rolling_7_likes
    , rolling_7_likes - last_7_likes as wow_likes
    , rolling_7_recasts
    , rolling_7_recasts - last_7_recasts as wow_recasts
    , '||' as split_2
    , st.avg_txs
    , st.avg_volume_usd
    , st.avg_contracts_deployed
    , cd.top_domains
    , slice(st.top_casters,1,3) as top_casters
    , all_caster_fids
FROM all_channels ac
LEFT JOIN cast_stats st ON ac.parent_url = st.parent_url
LEFT JOIN channel_domains cd On ac.parent_url = cd.parent_url
LEFT JOIN dune.dune.dataset_farcaster_early_channels purl ON purl.parent_url = ac.parent_url
order by engagement desc