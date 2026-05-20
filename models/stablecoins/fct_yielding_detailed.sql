{{ config(
    materialized='table',
    unique_key=['dt', 'blockchain', 'token_address']
) }}


with
    stable_rwa as (
        select * from {{ ref('fct_yielding_evm_stablecoins')}} -- Yielding Stablecoins
        union all
        select * from {{ ref('fct_yielding_rwas')}} -- Yielding RWAs
    ),
    -- aggregated daily usd value
    stable_rwa_agg as (
        select
            dt,
            sum(coalesce(value_usd, 0)) as value_usd_total
        from stable_rwa
        group by 1
    ),
    global as (
        select
            sr.blockchain,
            dt,
            sr.protocol,
            sr.category,
            sr.token_name,
            case
                when sr.category = 'rwa' then null
                when lower(sr.token_name) like '%usdc%' then 'USDC'
                when lower(sr.token_name) like '%usdt%' then 'USDT'
                when lower(sr.token_name) like '%usde%' then 'USDe'
                when lower(sr.token_name) like '%usds%' then 'USDS'
                when lower(sr.token_name) like '%usd0%' then 'USD0'
                when lower(sr.token_name) like '%dai%' then 'DAI'
                else 'Others'
            end as stable_token,
            sr.token_address,
            sr.amount,
            sr.value_usd,
            ag.value_usd_total
        from stable_rwa sr
        left join stable_rwa_agg ag using (dt)
    )

select * from global
where dt >= date '2020-01-01'
order by dt desc, value_usd desc