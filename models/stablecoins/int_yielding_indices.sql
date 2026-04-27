{#
    key notes on yielding rwas model:
    - Uses merge strategy with custom deduplication logic

    - On incremental runs, filters out rows that already exist in the target table
    - Checks target table using unique key columns to prevent duplicates
#}


{{ config(
    alias = 'int_yielding_indices'
    , materialized = 'table'
    , enabled=true
)}}

WITH
    tokens as (
    select protocol, category, chain, asset_type, from_hex(token_address) as token_address, token_name, token_decimals, from_hex(underlying_token_address) as underlying_token_address, underlying_token_name, underlying_token_decimals, start_date
    from {{ref('erc20_yielding_stablecoins')}}
)
, seq as (
    select
        t.chain,
        t.category,
        t.protocol,
        t.token_address,
        t.token_name,
        t.underlying_token_address,
        t.underlying_token_name,
        s.dt
    from (
        select chain, category, protocol, token_address, token_name, underlying_token_address, underlying_token_name, min(start_date) as start_date
        from tokens group by 1,2,3,4,5,6,7
    ) t
    cross join unnest(sequence(t.start_date, current_date, interval '1' day)) as s(dt)
)
, aave_indices_v2 as (
    select
        chain,
        'aave-v2' as protocol,
        contract_address,
        reserve as underlying_token_address,
        evt_block_date as dt,
        max_by(liquidityIndex / 1e27, evt_block_time) as index
    from {{source('aave_v2_multichain', 'lendingpool_evt_reservedataupdated')}}
    WHERE 1=1 
    {% if is_incremental() %}
        AND evt_block_date >= date_trunc('day', NOW() - interval'1' day)
    {% endif %}
    group by 1, 3, 4, 5
),
-- get aave supply indices per reserve and backfill it if no pool updates
aave_indices_v3 as (

    SELECT chain, 'aave-v3' as protocol, contract_address, underlying_token_address, dt, index 
    FROM (
        select
            chain,
            contract_address,
            reserve as underlying_token_address,
            evt_block_date as dt,
            max_by(liquidityIndex / 1e27, evt_block_time) as index
        from {{source('aave_v3_multichain', 'pool_evt_ReserveDataUpdated')}} -- ethereum, avalanche
        WHERE 1=1  and chain in ('avalanche_c', 'bnb', 'ethereum', 'polygon')
        {% if is_incremental() %}
            AND evt_block_date >= date_trunc('day', NOW() - interval'1' day)
        {% endif %}
        group by 1, 2, 3, 4
        union all
        select
            chain,
            contract_address,
            reserve as underlying_token_address,
            evt_block_date as dt,
            max_by(liquidityIndex / 1e27, evt_block_time) as index
        from {{source('aave_v3_multichain', 'l2pool_evt_reservedataupdated')}} -- arbitrum 
        WHERE 1=1 and chain in ('arbitrum', 'base', 'ink')
        {% if is_incremental() %}
            AND evt_block_date >= date_trunc('day', NOW() - interval'1' day)
        {% endif %}
        group by 1, 2, 3, 4
        union all
        select
            'plasma' as chain,
            contract_address,
            reserve as underlying_token_address,
            evt_block_date as dt,
            max_by(liquidityIndex / 1e27, evt_block_time) as index
        from  {{source('aave_v3_plasma', 'poolinstance_evt_reservedataupdated')}} -- plasma
        WHERE 1=1 
        {% if is_incremental() %}
            AND evt_block_date >= date_trunc('day', NOW() - interval'1' day)
        {% endif %}
        group by 2, 3, 4
    )
)
, sky as (

    select  'ethereum' as chain, 'sky' as protocol, contract_address, 0xdc035d45d973e3ec169d2276ddab16f1e407384f as underlying_token_address
        , evt_block_date as dt, MAX_BY(chi / 1e27, evt_block_time) as index
    FROM {{ source('sky_ethereum', 'susds_evt_drip')}}
    WHERE 1=1 
    {% if is_incremental() %}
        AND evt_block_date >= date_trunc('day', NOW() - interval'1' day)
    {% endif %}
    GROUP BY 3, 5
)
, maker as (

    select 'ethereum' as chain , 'sky' as protocol, contract_address, 0x6b175474e89094c44da98b954eedeac495271d0f as underlying_token_address
        , call_block_date as dt, MAX_BY(output_tmp / 1e27, call_block_time) as index
    FROM {{ source('maker_ethereum', 'pot_call_drip')}}
    where call_success = true
    {% if is_incremental() %}
        AND call_block_date >= date_trunc('day', NOW() - interval'1' day)
    {% endif %}
    GROUP BY 3, 5
)
-- ************************************************************************************************************************
-- *********************************     C O M P O U N D  V3 -  cUSDC, cUSDT       ****************************************
-- ************************************************************************************************************************
, compound_v3 as (
    SELECT period as dt, blockchain, 'compound-v3' as protocol, token_address, asset_address, supply_index as index
    FROM {{source('steakhouse', 'result_compound_v3_markets_data')}} as c
        JOIN tokens as t on c.asset_address = t.underlying_token_address and c.blockchain = t.chain
            AND t.protocol = 'compound-v3'
)
-- -- ************************************************************************************************************************
-- -- ******************************************          M O R P H O           **********************************************
-- -- ************************************************************************************************************************
, morpho_stables_100k as (
    select distinct blockchain, vault_address
    from {{source('steakhouse', 'result_morpho_vaults_data')}}
    where dt = (select max(dt) as dt_last from {{source('steakhouse', 'result_morpho_vaults_data')}})
        and supply_usd >= 1e5
        and token_symbol like '%USD%'
)
, morpho_vaults as (
    select dt, blockchain, 'morpho-v1' as protocol, vault_address, share_price_usd as index 
    from {{source('steakhouse', 'result_morpho_vaults_data')}} as v
        JOIN morpho_stables_100k as m using (blockchain, vault_address)
)

SELECT dt, blockchain, protocol, token_address, index
FROM compound_v3
union all
SELECT dt, blockchain, protocol, vault_address, index
FROM morpho_vaults
union all
SELECT seq.dt
    , seq.chain as blockchain
    , seq.protocol
    , seq.token_address
    , coalesce(i.index, last_value(i.index) ignore nulls over (partition by seq.chain, seq.protocol, seq.token_address order by seq.dt)) as index
FROM seq LEFT JOIN (
    SELECT *
    FROM aave_indices_v2
    UNION ALL
    SELECT *
    FROM aave_indices_v3
    UNION ALL
    SELECT *
    FROM sky
    UNION ALL
    SELECT *
    FROM maker
) as i on seq.dt = i.dt
    and seq.protocol = i.protocol
    and seq.chain = i.chain
    and seq.token_address = i.contract_address