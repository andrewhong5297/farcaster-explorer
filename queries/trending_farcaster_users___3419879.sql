-- part of a query repo
-- query name: Trending Farcaster Users
-- query link: https://dune.com/queries/3419879


with 
    user_targets as (
        SELECT
            distinct
            re.fid
            , c.fid as target_fid
            , 'reply' as action
        FROM dune.neynar.dataset_farcaster_casts re 
        LEFT JOIN dune.neynar.dataset_farcaster_casts c ON re.parent_hash = c.hash
        WHERE re.fid = (SELECT distinct fid FROM dune.neynar.dataset_farcaster_profile_with_addresses WHERE fname = '{{track user}}' order by fid asc limit 1)
        AND re.deleted_at is null
        AND re.created_at >= now() - interval '14' day
        
        UNION ALL 
        
        SELECT 
            distinct 
            fid
            , target_fid
            , 'follow' as action
        FROM dune.neynar.dataset_farcaster_links
        WHERE fid = (SELECT distinct fid FROM dune.neynar.dataset_farcaster_profile_with_addresses WHERE fname = '{{track user}}' order by fid asc limit 1)
        AND deleted_at is null
        AND created_at >= now() - interval '14' day
        
        UNION ALL 
        
        SELECT 
            distinct 
            fid
            , target_fid
            , 'react' as action
        FROM dune.neynar.dataset_farcaster_reactions
        WHERE fid = (SELECT distinct fid FROM dune.neynar.dataset_farcaster_profile_with_addresses WHERE fname = '{{track user}}' order by fid asc limit 1)
        AND deleted_at is null
        AND created_at >= now() - interval '14' day
    )
    
    , onchain_filters as (
       --get all fids that hold a given token
        with
        fids as (
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
            distinct fid
        FROM dune.dune.result_wallet_all_chain_activity_summary s
        JOIN fids f ON s.wallet = f.address
        WHERE '{{held token address}}' != 'none'
        AND contains(concat(erc20_addresses,nft_addresses),try(from_hex('{{held token address}}')))
    )

SELECT
*
FROM query_3418402 q
WHERE account_age <= {{user days old}}
AND ('{{user channel filter}}' = 'all channels'
OR any_match(all_channels, x -> lower(x) LIKE '%' || lower('{{user channel filter}}') || '%')
)
--filter for fids that a given user has replied to, liked, recasted, or followed
AND ('{{track user}}' = 'no filter' 
OR q.fid IN (SELECT distinct target_fid FROM user_targets WHERE '{{track user action}}' = 'all' OR '{{track user action}}' = action)
)
--filter for users who are holding a given token
AND ('{{held token address}}' = 'none' 
OR q.fid IN (SELECT fid FROM onchain_filters)
)
--filter for users of a given blockchain? probably not needed right now.
--for a channel, do an approx_percentile on txs on only verified accounts and tier 1+
order by {{user sort by}} desc
LIMIT 1000