{#
    key notes on yielding rwas model:
#}


{{config(
    alias = 'fct_yielding_rwas'
    , materialized = 'view'
    , enabled=true

)}}


WITH combined_transfers as (
    SELECT * FROM {{ ref('dbt_yielding_evm_rwas')}}
    -- union all
    -- SELECT 'spl' as token_type, * FROM {{ ref('dbt_yielding_spl_rwas')}}
)
, all_balance as (
    SELECT blockchain, dt, protocol, token_name, token_address, daily_net_amount
        , SUM(daily_net_amount) over (partition by blockchain, token_address order by dt) as balance
    FROM combined_transfers
)
, all_balance_backfill as (

    SELECT blockchain, dt, protocol, 'rwa' as category
        , token_name, token_address, balance, price_usd
        , price_usd * balance as value_usd
    FROM (
        SELECT blockchain, dt, protocol
            , token_name, ab.token_address, balance
            , CASE
                WHEN token_name in ('BUIDL', 'BUIDL-I', 'BENJI', 'WTGXX') THEN 1
                ELSE p.price_usd
            END as price_usd
        FROM all_balance AS ab
        left join {{source('steakhouse', 'result_token_price')}} p using (blockchain, dt)
        where (ab.token_address = 0xe86845788d6e3e5c2393ade1a051ae617d974c09 and p.token_address = 0x96f6ef951840721adbf46ac996b59e0235cb985c) or ab.token_address = p.token_address 
    )
)
select *
from all_balance_backfill
order by dt desc, value_usd desc