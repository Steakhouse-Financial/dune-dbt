-- {#
--     key notes on append model:
--     - Uses append strategy with custom deduplication logic
--     - On incremental runs, filters out rows that already exist in the target table
--     - Checks target table using unique key columns to prevent duplicates
-- #}


{{ config(
    alias = 'dbt_yielding_erc20_stablecoins'
    , enabled=false
    , materialized = 'incremental'
    , incremental_strategy = 'delete+insert'
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
            m.evt_block_date as dt,
            tk.token_address,
            (m."value"/ power(10, tk.token_decimals)) / (tr.index / 1e27) as amount
        from {{ source('aave_v2_multichain', 'atoken_evt_mint')}} m
        join tokens tk
            on tr.chain = tk.chain and tr.contract_address = tk.token_address
        where tk.protocol = 'aave-v2'
        union all
        select
            tr.chain,
            b.evt_block_date as dt,
            tk.token_address,
            -(tr."value" / power(10, tk.token_decimals)) / (tr.index / 1e27) as amount
        from {{ source('aave_v2_multichain', 'atoken_evt_burn')}} b
        join tokens tk
            on tr.chain = tk.chain and tr.contract_address = tk.token_address
        where tk.protocol = 'aave-v2'
    )
    group by 1,2,3
),
-- get aave supply indices per reserve and backfill it if no pool updates
aave_indices_v2 as (
    select
        chain,
        underlying_token_address,
        dt,
        last_value(coalesce(l.index, 1)) ignore nulls over (partition by chain, underlying_token_address order by dt asc rows between unbounded preceding and current row) as index
    from seq s
    left join (
        select
            chain,
            reserve as underlying_token_address,
            date(evt_block_time) as dt,
            max(liquidityIndex / 1e27) as index
        from aave_v2_multichain.lendingpool_evt_reservedataupdated
        group by 1,2,3
    ) l using (chain, underlying_token_address, dt)
    where s.protocol = 'aave-v2'
),
-- calculate the atoken balance taking into account their yield (therefore, multiplying by the latest daily supply index)
aave_balance_v2 as (
    select
        chain,
        dt,
        tk.protocol,
        tk.category,
        tk.token_name,
        token_address,
        sum(coalesce(t.amount, 0)) over (partition by chain, token_address order by dt asc) * i.index as amount
    from seq s
    join tokens tk using (chain, token_address, underlying_token_address)
    left join aave_transfers_v2 t using (chain, token_address, dt)
    left join aave_indices_v2 i using (chain, underlying_token_address, dt)
    where s.protocol = 'aave-v2'
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
    )
    group by 1,2,3
),
-- get aave supply indices per reserve and backfill it if no pool updates
aave_indices_v3 as (
    select
        chain,
        underlying_token_address,
        dt,
        last_value(l.index) ignore nulls over (partition by chain, underlying_token_address order by dt asc rows between unbounded preceding and current row) as index
    from seq s
    left join (
        select
            chain,
            reserve as underlying_token_address,
            date(evt_block_time) as dt,
            max(liquidityIndex / 1e27) as index
        from aave_v3_multichain.pool_evt_ReserveDataUpdated -- ethereum, avalanche
        group by 1,2,3
        union all
        select
            'arbitrum' as chain,
            reserve as underlying_token_address,
            date(evt_block_time) as dt,
            max(liquidityIndex / 1e27) as index
        from aave_v3_arbitrum.l2pool_evt_reservedataupdated -- arbitrum
        group by 1,2,3
        union all
        select
            'base' as chain,
            reserve as underlying_token_address,
            date(evt_block_time) as dt,
            max(liquidityIndex / 1e27) as index
        from aave_v3_base.l2pool_evt_reservedataupdated -- base
        group by 1,2,3
    ) l using (chain, underlying_token_address, dt)
    where s.protocol = 'aave-v3'
),
-- calculate the atoken balance taking into account their yield (therefore, multiplying by the latest daily supply index)
aave_balance_v3 as (
    select
        chain,
        dt,
        tk.protocol,
        tk.category,
        tk.token_name,
        token_address,
        sum(coalesce(t.amount, 0)) over (partition by chain, token_address order by dt asc) * i.index as amount
    from seq s
    join tokens tk using (chain, token_address, underlying_token_address)
    left join aave_transfers_v3 t using (chain, token_address, dt)
    left join aave_indices_v3 i using (chain, underlying_token_address, dt)
    where s.protocol = 'aave-v3'
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
            date(evt_block_time) as dt,
            shares as amount
        from {{ source('maker_ethereum', 'SavingsDai_evt_Deposit')}}
        union all
        select
            contract_address as token_address,
            date(evt_block_time) as dt,
            -shares as amount
        from {{ source('maker_ethereum', 'SavingsDai_evt_Withdraw')}}
    )
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
    from ethena_labs_ethereum.stakedusdev2_evt_transfer
    where 0x0000000000000000000000000000000000000000 in ("from", "to")
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
    from elixir_ethereum.stdeusd_evt_transfer
    where 0x0000000000000000000000000000000000000000 in ("from", "to")
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
    from reservoir_protocol_ethereum.savingcoin_evt_transfer
    where 0x0000000000000000000000000000000000000000 in ("from", "to")
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
    from reservoir_protocol_ethereum.savingcoinv2_evt_transfer
    where 0x0000000000000000000000000000000000000000 in ("from", "to")
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
    from maplefinance_v2_ethereum.pool_v2_evt_transfer tr
    join tokens tk
        on tr.contract_address = tk.token_address
    where tk.protocol = 'maple'
        and 0x0000000000000000000000000000000000000000 in (tr.owner_, tr.recipient_)
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
            select evt_block_time, contract_address, mintTokens from compound_v2_ethereum.cerc20_evt_mint
            union all
            select evt_block_time, contract_address, mintTokens from compound_v2_ethereum.cerc20delegator_evt_mint
        ) tr
        join tokens tk on tr.contract_address = tk.token_address
        where tk.protocol = 'compound-v2'
        union all
        select
            tr.contract_address as token_address,
            date(tr.evt_block_time) as dt,
            -tr.redeemTokens as amount
        from (
            select evt_block_time, contract_address, redeemTokens from compound_v2_ethereum.cerc20_evt_redeem
            union all
            select evt_block_time, contract_address, redeemTokens from compound_v2_ethereum.cerc20delegator_evt_redeem
        ) tr
        join tokens tk on tr.contract_address = tk.token_address
        where tk.protocol = 'compound-v2'
    )
    group by 1,2
),
-- ************************************************************************************************************************
-- *********************************     C O M P O U N D  V3 -  cUSDC, cUSDT       ****************************************
-- ************************************************************************************************************************
compound_balance_v3 as (
    select
        s.chain,
        s.dt,
        tk.protocol,
        tk.category,
        tk.token_name,
        tk.token_address,
        coalesce(t.supply_tokens, 0) as amount,
        coalesce(t.supply_usd, 0) as value_usd
    from seq s
    join tokens tk
        on s.chain = tk.chain
        and s.token_address = tk.token_address
        and s.underlying_token_address = tk.underlying_token_address
    left join dune.steakhouse.result_compound_v3_markets_data t
        on s.chain = t.blockchain
        and s.underlying_token_name = t.symbol
        and s.dt = t.period
    where s.protocol = 'compound-v3'
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
    from usual_ethereum.usd0pp_evt_transfer tr
    join tokens tk
        on tr.contract_address = tk.token_address
    where tk.protocol = 'usual'
        and 0x0000000000000000000000000000000000000000 in (tr."from", tr."to")
    group by 1,2
)