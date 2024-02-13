-- part of a query repo
-- query name: Trending Farcaster Channels
-- query link: https://dune.com/queries/3422001


with 
    get_fid as (
        SELECT 
        fid
        FROM dune.neynar.dataset_farcaster_profile_with_addresses pf
        -- LEFT JOIN dune.neynar.dataset_farcaster_fids f ON pf.fid = f.fid
        WHERE fname = lower('{{username filter}}')
        order by fid asc
        limit 1
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
            distinct f.fid
        FROM dune.dune.result_wallet_all_chain_activity_summary s
        JOIN fids f ON s.wallet = f.address
        WHERE '{{channel held token address}}' != 'none'
        AND contains(concat(erc20_addresses,nft_addresses),try(from_hex('{{channel held token address}}')))
    )

SELECT
*
FROM query_3418331
WHERE channel_age <= {{channel days old}}
--filter for channels a given user is active it
AND ('{{username filter}}' = 'all users'
OR contains(all_caster_fids, (SELECT fid FROM get_fid))
)
--filter for channels where at least one active caster holds the token
AND ('{{channel held token address}}' = 'none'
OR contains(all_caster_fids, (SELECT fid FROM onchain_filters))
)
order by {{channel sort by}} desc
LIMIT 1000