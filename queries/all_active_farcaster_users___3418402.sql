-- part of a query repo
-- query name: All Active Farcaster Users
-- query link: https://dune.com/queries/3418402


with 
    fol as (
        SELECT 
            target_fid as fid
            , count(*) as followers
            , sum(case when created_at >= now() - interval '7' day then 1 else 0 end) as wow_followers
        FROM dune.neynar.dataset_farcaster_links
        WHERE deleted_at is null
        GROUP BY 1
    )
    
    , reactions as (
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
    
    , active_casters as (
        with 
            --doing this just to be able to order channels in array
            base as ( 
                SELECT
                    c.fid
                    , COALESCE(c.parent_url,c.root_parent_url) as channel_url
                    , approx_distinct(case when c.created_at >= now() - interval '7' day then c.hash else null end) as casts
                    , approx_distinct(case when c.created_at >= now() - interval '7' day then re.hash else null end) as got_replies
                    , sum(case when c.created_at >= now() - interval '7' day then r.got_likes else 0 end) as got_likes
                    , sum(case when c.created_at >= now() - interval '7' day then r.got_recasts else 0 end) as got_recasts
                    , approx_distinct(case when c.created_at between (now() - interval '14' day) and (now() - interval '7' day) then c.hash else null end) as casts_last
                    , approx_distinct(case when c.created_at between (now() - interval '14' day) and (now() - interval '7' day) then re.hash else null end) as got_replies_last
                    , sum(case when c.created_at between (now() - interval '14' day) and (now() - interval '7' day) then r.got_likes else 0 end) as got_likes_last
                    , sum(case when c.created_at between (now() - interval '14' day) and (now() - interval '7' day) then r.got_recasts else 0 end) as got_recasts_last
                    --could add last week casts/replies/likes/recasts later on, but followers is strong indicator for now.
                FROM dune.neynar.dataset_farcaster_casts c
                LEFT JOIN reactions r ON r.target_hash = c.hash 
                LEFT JOIN dune.neynar.dataset_farcaster_casts re ON re.root_parent_hash = c.hash 
                    AND re.deleted_at is null 
                    AND re.fid != c.fid 
                    AND re.created_at >= now() - interval '14' day
                WHERE c.deleted_at is null
                and c.created_at >= now() - interval '14' day
                GROUP BY 1,2
            )
            
        SELECT 
            fid
            , array_agg(
                COALESCE(purl.channel_id,try(split(b.channel_url,'/')[6]), b.channel_url)
                order by casts desc) FILTER (WHERE channel_url is not null) as channel_urls
            , sum(casts) as casts
            , sum(got_replies) as got_replies
            , sum(got_likes) as got_likes
            , sum(got_recasts) as got_recasts
            , sum(casts_last) as casts_last
            , sum(got_replies_last) as got_replies_last
            , sum(got_likes_last) as got_likes_last
            , sum(got_recasts_last) as got_recasts_last
        FROM base b
        LEFT JOIN dune.dune.dataset_farcaster_early_channels purl ON purl.parent_url = b.channel_url
        GROUP BY 1
    )
    
    , user_domains as (
        with base as (
            SELECT
                c.fid
                , replace(split(regexp_extract(text, 'https://\S*'),'/')[3],'www.','') as domain
                , count(*) as ct
            FROM dune.neynar.dataset_farcaster_casts c
            WHERE c.deleted_at is null
            and c.created_at >= now() - interval '14' day --whole query is 14 day trends
            GROUP BY 1,2
        )
        
        SELECT 
            fid
            , filter(slice(array_agg(domain order by ct desc),1,3),x-> x is not null) as top_domains 
        FROM base
        WHERE domain NOT IN ('warpcast.com','x.com','twitter.com')
        group by 1
    )
    
    , user_blockchain_stats as (
        with fids as (
            SELECT 
            fid
            , custody_address as address
            FROM dune.neynar.dataset_farcaster_fids
            
            UNION ALL
            
            SELECT 
            fid
            , from_hex(json_value(claim, 'strict $.address')) as address
            FROM dune.neynar.dataset_farcaster_verifications
        )
        
        SELECT 
        f.fid
        , sum(st.txs) as total_transactions
        , sum(nft_volume_usd + dex_volume_usd) as trading_volume_usd
        , sum(st.contracts_deployed) as contracts_deployed
        , array_agg(address) as addresses
        FROM fids f
        LEFT JOIN dune.dune.result_wallet_all_chain_activity_summary st ON f.address = st.wallet
        group by 1
    )

SELECT 
case --the below tiers are somewhat arbitrary, the follow a power law distribution as one would expect of a social network. 
    when fol.followers >=50000 and ac.casts >=10 and (ac.got_likes + ac.got_recasts*5 + ac.got_replies*10) >= 50000 then '💎 vip'
    when fol.followers >=10000 and ac.casts >=10 and (ac.got_likes + ac.got_recasts*5 + ac.got_replies*10) >= 25000 then '🥇 influencer'
    when fol.followers >=1000 and ac.casts >=5 and (ac.got_likes + ac.got_recasts*5 + ac.got_replies*10) >= 5000 then '🥈 star'
    when fol.followers >=400 and ac.casts >=1 and (ac.got_likes + ac.got_recasts*5 + ac.got_replies*10) >= 500 then '🥉 active'
    else '🤖 npc'
end as fid_active_tier_name
, case --the below tiers are somewhat arbitrary, the follow a power law distribution as one would expect of a social network. 
    when fol.followers >=50000 and ac.casts >=10 and (ac.got_likes + ac.got_recasts*5 + ac.got_replies*10) >= 50000 then 4
    when fol.followers >=10000 and ac.casts >=10 and (ac.got_likes + ac.got_recasts*5 + ac.got_replies*10) >= 25000 then 3
    when fol.followers >=1000 and ac.casts >=5 and (ac.got_likes + ac.got_recasts*5 + ac.got_replies*10) >= 5000 then 2
    when fol.followers >=400 and ac.casts >=1 and (ac.got_likes + ac.got_recasts*5 + ac.got_replies*10) >= 500 then 1
    else 0
end as fid_active_tier
, case --the below tiers are somewhat arbitrary, the follow a power law distribution as one would expect of a social network. 
    when fol.followers - fol.wow_followers >=50000 and ac.casts_last >=10 and (ac.got_likes_last + ac.got_recasts_last*5 + ac.got_replies_last*10) >= 50000 then 4
    when fol.followers - fol.wow_followers >=10000 and ac.casts_last >=10 and (ac.got_likes_last + ac.got_recasts_last*5 + ac.got_replies_last*10) >= 25000 then 3
    when fol.followers - fol.wow_followers >=1000 and ac.casts_last >=5 and (ac.got_likes_last + ac.got_recasts_last*5 + ac.got_replies_last*10) >= 5000 then 2
    when fol.followers - fol.wow_followers >=400 and ac.casts_last >=1 and (ac.got_likes_last + ac.got_recasts_last*5 + ac.got_replies_last*10) >= 100 then 1
    else 0
end as fid_active_tier_last
, ac.fid
, pf.fname
, get_href('https://warpcast.com/' || pf.fname, pf.fname) as fname_link
-- , f.created_at as signed_up_at
, date_diff('day',f.created_at,now()) as account_age
, cardinality(ac.channel_urls) as channels
, slice(ac.channel_urls,1,3) as top_channels
, slice(dm.top_domains,1,2) as top_domains
, COALESCE(fol.followers,0) as followers
, COALESCE(fol.wow_followers,0) as wow_followers
, ac.casts
, ac.casts - ac.casts_last as wow_casts
, ac.got_likes + ac.got_recasts*3 + ac.got_replies*10 as engagement
, (ac.got_likes + ac.got_recasts*3 + ac.got_replies*10) - (ac.got_likes_last + ac.got_recasts_last*3 + ac.got_replies_last*10) as wow_engage
, COALESCE(block.total_transactions,0) as total_transactions
, COALESCE(block.trading_volume_usd,0) as trading_volume_usd
, COALESCE(block.contracts_deployed,0) as contracts_deployed
, '||' as split_0
, ac.got_likes
, ac.got_likes - ac.got_likes_last as wow_likes
, ac.got_recasts
, ac.got_recasts - ac.got_recasts_last as wow_recasts
, ac.got_replies
, ac.got_replies - ac.got_replies_last as wow_replies
, block.addresses
, ac.channel_urls as all_channels
, dm.top_domains as all_domains
FROM active_casters ac
LEFT JOIN fol ON ac.fid = fol.fid
LEFT JOIN (SELECT distinct fid, fname, verified_addresses FROM dune.neynar.dataset_farcaster_profile_with_addresses) pf ON pf.fid = ac.fid
LEFT JOIN dune.neynar.dataset_farcaster_fids f ON f.fid = ac.fid
LEFT JOIN user_blockchain_stats block ON block.fid = ac.fid
LEFT JOIN user_domains dm ON dm.fid = ac.fid
order by followers desc