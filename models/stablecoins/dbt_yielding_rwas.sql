{#
    key notes on yielding rwas model:
#}


{{config(
    alias = 'dbt_yielding_rwas'
    , materialized = 'view'
    , enabled=false

)}}


WITH combined_transfers as (
    SELECT 'evm' as token_type, * FROM {{ ref('dbt_yielding_erc20_rwas')}}
    union all
    SELECT 'spl' as token_type, * FROM {{ ref('dbt_yielding_spl_rwas')}}
)