{#
    key notes on yielding rwas model:
    - Uses merge strategy with custom deduplication logic

    - On incremental runs, filters out rows that already exist in the target table
    - Checks target table using unique key columns to prevent duplicates
#}


{{ config(
    alias = 'dbt_yielding_spl_rwas'
    , materialized = 'incremental'
    , incremental_strategy = 'delete+insert'
    , properties = {
        "partitioned_by": "ARRAY['dt', 'token_address']"
    }
)
}}

with spl_tokens as (
    select protocol, mint_address as token_address, token_name, token_decimals, start_date
    from {{ref('spl_rwa_tokens')}}
)
, unioned_tokens as (
    select protocol, 'solana' as blockchain, token_address, token_name, token_decimals, start_date from spl_tokens
)
-- generate time sequence for each chain and token starting from their creation date
, seq as (
    select
        t.blockchain,
        t.protocol,
        t.token_address,
        t.token_name,
        date(s.dt) as dt
    from (
        select blockchain, protocol, token_address, token_name, start_date
        from unioned_tokens
    ) t
    cross join unnest(sequence(t.start_date, current_date, interval '1' day)) as s(dt)
)
, rwa_transfers_sol as (
    select
        tr.block_date as dt,
        tr.token_mint_address as token_address,
        'solana' as blockchain,
        sum(
            case
                when tr.action = 'mint' then amount
                when tr.action = 'burn' then -amount
            end / power(10, tk.token_decimals)
        ) as amount
    from {{ source('tokens_solana', 'transfers') }} as tr
    join spl_tokens as tk
        on tr.token_mint_address = tk.token_address
    where tr.block_date >= tk.start_date
        {% if is_incremental() %}
            AND tr.block_date >= date_trunc('day', NOW() - interval'1' day)
        {% endif %}
    group by 1, 2
),
rwa_daily as (
    select
        seq.blockchain,
        seq.dt,
        seq.protocol,
        seq.token_name,
        seq.token_address,
        coalesce(tr.amount, 0) as daily_net_amount
    from seq left join (
        select * from rwa_transfers_sol
    ) tr on seq.dt = tr.dt and seq.token_address = tr.token_address
    and seq.blockchain = tr.blockchain
)
select * 
from rwa_daily

