-- {#
--     key notes on append model:
--     - Uses append strategy with custom deduplication logic
--     - On incremental runs, filters out rows that already exist in the target table
--     - Checks target table using unique key columns to prevent duplicates
-- #}


{{ config(
    alias = 'dbt_yielding_evm_stablecoins'
    , enabled=true
    , materialized = 'incremental'
    , incremental_strategy = 'delete+insert'
    , unique_key = ['dt', 'blockchain', 'token_address']
    , properties = {
        "partitioned_by": "ARRAY['dt', 'blockchain']"
    }
)
}}


with
    tokens as (
    select protocol, category, chain, asset_type, from_hex(token_address) as token_address, token_name, token_decimals, underlying_token_address, underlying_token_name, underlying_token_decimals, start_date
    from {{ref('erc20_yielding_stablecoins')}}
)
-- generate time sequence for each chain and token starting from their creation date
, seq as (
    select
        t.chain,
        t.category,
        t.protocol,
        t.token_address,
        t.token_name,
        t.underlying_token_address,
        t.underlying_token_name,
        date(s.dt) as dt
    from (
        select chain, category, protocol, token_address, token_name, underlying_token_address, underlying_token_name, min(start_date) as start_date
        from tokens group by 1,2,3,4,5,6,7
    ) t
    cross join unnest(sequence(t.start_date, current_date, interval '1' day)) as s(dt)
),
-- ************************************************************************************************************************
-- *********************************                     A A V E  V2                   ************************************
-- ************************************************************************************************************************
aave_transfers_v2 as (
    select
        chain,
        dt,
        token_address,
        sum(amount) as amount
    from (
        select
            tr.chain,
            tr.evt_block_date as dt,
            tk.token_address,
            (tr."value"/ power(10, tk.token_decimals)) / (tr.index / 1e27) as amount
        from {{ source('aave_v2_multichain', 'atoken_evt_mint')}} tr
        join tokens tk
            on tr.chain = tk.chain and tr.contract_address = tk.token_address
        where tk.protocol = 'aave-v2'
        {% if is_incremental() %}
            AND tr.evt_block_date >= date_trunc('day', NOW() - interval'1' day)
        {% endif %}
        union all
        select
            tr.chain,
            tr.evt_block_date as dt,
            tk.token_address,
            -(tr."value" / power(10, tk.token_decimals)) / (tr.index / 1e27) as amount
        from {{ source('aave_v2_multichain', 'atoken_evt_burn')}} tr
        join tokens tk
            on tr.chain = tk.chain and tr.contract_address = tk.token_address
        where tk.protocol = 'aave-v2'
        {% if is_incremental() %}
            AND tr.evt_block_date >= date_trunc('day', NOW() - interval'1' day)
        {% endif %}
    )
    group by 1,2,3
),
-- ************************************************************************************************************************
-- *********************************                     A A V E  V3                   ************************************
-- ************************************************************************************************************************
-- get aToken transfers. The amount calc needs to be converted to int256 to avoid overflow with negative amounts
aave_transfers_v3 as (
    select
        chain,
        dt,
        token_address,
        sum(amount) as amount
    from (
        select
            m.chain,
            m.evt_block_date as dt,
            tk.token_address,
            ((cast(m."value" as int256) - cast(m."balanceIncrease" as int256)) / power(10, tk.token_decimals)) / (m.index / 1e27) as amount
        from {{ source('aave_v3_multichain', 'atoken_evt_mint')}} m
        join tokens tk
            on m.chain = tk.chain
            and m.contract_address = tk.token_address
            where tk.protocol = 'aave-v3'
        {% if is_incremental() %}
            AND m.evt_block_date >= date_trunc('day', NOW() - interval'1' day)
        {% endif %}
    
        union all
        select
            b.chain,
            b.evt_block_date as dt,
            tk.token_address,
            -((cast(b."value" as int256) + cast(b."balanceIncrease" as int256)) / power(10, tk.token_decimals)) / (b.index / 1e27) as amount
        from {{ source('aave_v3_multichain', 'atoken_evt_burn')}} b
        join tokens tk
            on b.chain = tk.chain
            and b.contract_address = tk.token_address
            where tk.protocol = 'aave-v3'
        {% if is_incremental() %}
            AND b.evt_block_date >= date_trunc('day', NOW() - interval'1' day)
        {% endif %}
    )
    group by 1,2,3
),
-- ************************************************************************************************************************
-- ***********************************             S K Y  -  sUSDS, sDAI             **************************************
-- ************************************************************************************************************************
susds_transfers as (
    select
        evt_block_date as dt,
        contract_address as token_address,
        'ethereum' as chain,
        sum(
            case
                when "from" = 0x0000000000000000000000000000000000000000 then "value"
                when "to" = 0x0000000000000000000000000000000000000000 then -"value"
            end
        ) / 1e18 as amount
    from {{ source('sky_ethereum', 'susds_evt_transfer')}}
    where 0x0000000000000000000000000000000000000000 in ("from", "to")
    {% if is_incremental() %}
        AND evt_block_date >= date_trunc('day', NOW() - interval'1' day)
    {% endif %}
    group by 1,2
),
-- @dev: sDAI deposits & withdrawals in ethereum do not emit transfer event
sdai_transfers as (
    select
        dt,
        token_address,
        'ethereum' as chain,
        sum(amount / 1e18) as amount
    from (
        select
            contract_address as token_address,
            evt_block_date as dt,
            shares as amount
        from {{ source('maker_ethereum', 'SavingsDai_evt_Deposit')}}
        union all
        select
            contract_address as token_address,
            evt_block_date as dt,
            -shares as amount
        from {{ source('maker_ethereum', 'SavingsDai_evt_Withdraw')}}
    )
    WHERE 1=1
    {% if is_incremental() %}
        AND dt >= date_trunc('day', NOW() - interval'1' day)
    {% endif %}
    group by 1,2
),
-- ************************************************************************************************************************
-- ***********************************             E T H E N A  -  sUSDe             **************************************
-- ************************************************************************************************************************
usde_transfers as (
    select
        evt_block_date as dt,
        contract_address as token_address,
        'ethereum' as chain,
        sum(
            case
                when "from" = 0x0000000000000000000000000000000000000000 then "value"
                when "to" = 0x0000000000000000000000000000000000000000 then -"value"
            end
        ) / 1e18 as amount
    from {{ source('ethena_labs_ethereum', 'stakedusdev2_evt_transfer')}}
    where 0x0000000000000000000000000000000000000000 in ("from", "to")
    {% if is_incremental() %}
        AND evt_block_date >= date_trunc('day', NOW() - interval'1' day)
    {% endif %}
    group by 1,2
),
-- ************************************************************************************************************************
-- ***********************************            E L I X I R  -  sdeUSD            ***************************************
-- ************************************************************************************************************************
sdeusd_transfers as (
    select
        evt_block_date as dt,
        contract_address as token_address,
        'ethereum' as chain,
        sum(
            case
                when "from" = 0x0000000000000000000000000000000000000000 then "value"
                when "to" = 0x0000000000000000000000000000000000000000 then -"value"
            end
        ) / 1e18 as amount
    from  {{ source('elixir_ethereum', 'stdeusd_evt_transfer')}}
    where 0x0000000000000000000000000000000000000000 in ("from", "to")
    {% if is_incremental() %}
        AND evt_block_date >= date_trunc('day', NOW() - interval'1' day)
    {% endif %}
    group by 1,2
),
-- ************************************************************************************************************************
-- ***********************************         R E S E R V O I R  -  srUSD          ***************************************
-- ************************************************************************************************************************
srusd_transfers as (
    select
        evt_block_date as dt,
        contract_address as token_address,
        'ethereum' as chain,
        sum(
            case
                when "from" = 0x0000000000000000000000000000000000000000 then "value"
                when "to" = 0x0000000000000000000000000000000000000000 then -"value"
            end
        ) / 1e18 as amount
    from  {{ source('reservoir_protocol_ethereum', 'savingcoin_evt_transfer')}}
    where 0x0000000000000000000000000000000000000000 in ("from", "to")
    {% if is_incremental() %}
        AND evt_block_date >= date_trunc('day', NOW() - interval'1' day)
    {% endif %}
    group by 1,2
),
wsrusd_transfers as (
    select
        evt_block_date as dt,
        contract_address as token_address,
        'ethereum' as chain,
        sum(
            case
                when "from" = 0x0000000000000000000000000000000000000000 then "value"
                when "to" = 0x0000000000000000000000000000000000000000 then -"value"
            end
        ) / 1e18 as amount
    from {{ source('reservoir_protocol_ethereum', 'savingcoinv2_evt_transfer')}}
    where 0x0000000000000000000000000000000000000000 in ("from", "to")
    {% if is_incremental() %}
        AND evt_block_date >= date_trunc('day', NOW() - interval'1' day)
    {% endif %}
    group by 1,2
),
-- ************************************************************************************************************************
-- ***********************************     M A P L E  -  syrupUSDC, syrupUSDT       ***************************************
-- ************************************************************************************************************************
syrup_transfers as (
    select
        tr.evt_block_date as dt,
        tr.contract_address as token_address,
        'ethereum' as chain,
        sum(
            case
                when tr.owner_ = 0x0000000000000000000000000000000000000000 then amount_
                when tr.recipient_ = 0x0000000000000000000000000000000000000000 then -amount_
            end
        ) / 1e6 as amount
    from {{ source('maplefinance_v2_ethereum', 'pool_v2_evt_transfer')}} tr
    join tokens tk
        on tr.contract_address = tk.token_address
    where tk.protocol = 'maple'
        and 0x0000000000000000000000000000000000000000 in (tr.owner_, tr.recipient_)
    {% if is_incremental() %}
        AND tr.evt_block_date >= date_trunc('day', NOW() - interval'1' day)
    {% endif %}
    group by 1,2
),
-- ************************************************************************************************************************
-- *********************************     C O M P O U N D  V2 -  cUSDC, cUSDT       ****************************************
-- ************************************************************************************************************************
-- @dev: cUSDC/cUSDT deposits & withdrawals in ethereum do not emit transfer event from/to 0x
compound_transfers_v2 as (
    select
        dt,
        token_address,
        'ethereum' as chain,
        sum(amount / 1e8) as amount
    from (
        select
            tr.contract_address as token_address,
            date(tr.evt_block_time) as dt,
            tr.mintTokens as amount
        from (
            select evt_block_date, evt_block_time, contract_address, mintTokens
            from {{ source('compound_v2_ethereum', 'cerc20_evt_mint')}}
            union all
            select evt_block_date, evt_block_time, contract_address, mintTokens
            from {{ source('compound_v2_ethereum', 'cerc20delegator_evt_mint')}}
        ) tr
        join tokens tk on tr.contract_address = tk.token_address
        where tk.protocol = 'compound-v2'
        {% if is_incremental() %}
            AND tr.evt_block_date >= date_trunc('day', NOW() - interval'1' day)
        {% endif %}
    
        union all
        select
            tr.contract_address as token_address,
            date(tr.evt_block_time) as dt,
            -tr.redeemTokens as amount
        from (
            select evt_block_date, evt_block_time, contract_address, redeemTokens
            from {{ source('compound_v2_ethereum', 'cerc20_evt_redeem')}}
            union all
            select evt_block_date, evt_block_time, contract_address, redeemTokens
            from {{ source('compound_v2_ethereum', 'cerc20delegator_evt_redeem')}}
        ) tr
        join tokens tk on tr.contract_address = tk.token_address
        where tk.protocol = 'compound-v2'
        {% if is_incremental() %}
            AND tr.evt_block_date >= date_trunc('day', NOW() - interval'1' day)
        {% endif %}
    )
    group by 1,2
),
-- ************************************************************************************************************************
-- ******************************************      U S U A L  -  USD0++      **********************************************
-- ************************************************************************************************************************
usd0pp_transfers as (
    select
        evt_block_date as dt,
        contract_address as token_address,
        'ethereum' as chain,
        sum(
            case
                when "from" = 0x0000000000000000000000000000000000000000 then "value"
                when "to" = 0x0000000000000000000000000000000000000000 then -"value"
            end
        ) / 1e18 as amount
    from {{ source('usual_ethereum', 'usd0pp_evt_transfer')}} tr
    join tokens tk
        on tr.contract_address = tk.token_address
    where tk.protocol = 'usual'
        and 0x0000000000000000000000000000000000000000 in (tr."from", tr."to")
    {% if is_incremental() %}
        AND tr.evt_block_date >= date_trunc('day', NOW() - interval'1' day)
    {% endif %}
    group by 1,2
)
, all_transfers as (
    select chain, dt, token_address, amount from aave_transfers_v2
    union all
    select chain, dt, token_address, amount from aave_transfers_v3
    union all
    select chain, dt, token_address, amount from susds_transfers
    union all
    select chain, dt, token_address, amount from sdai_transfers
    union all
    select chain, dt, token_address, amount from usde_transfers
    union all
    select chain, dt, token_address, amount from sdeusd_transfers
    union all
    select chain, dt, token_address, amount from srusd_transfers
    union all
    select chain, dt, token_address, amount from wsrusd_transfers
    union all
    select chain, dt, token_address, amount from syrup_transfers
    union all
    select chain, dt, token_address, amount from compound_transfers_v2
    union all
    select chain, dt, token_address, amount from usd0pp_transfers
)
, yielding_daily as (
    SELECT 
        s.dt, s.chain as blockchain, s.category, s.protocol
        , s.token_address, s.token_name
        , s.underlying_token_address, s.underlying_token_name
        , coalesce(alt.amount, 0) as daily_net_amount
    FROM seq AS s LEFT JOIN all_transfers as alt on s.dt = alt.dt 
        and s.chain = alt.chain and s.token_address = alt.token_address 

)
select *
from yielding_daily