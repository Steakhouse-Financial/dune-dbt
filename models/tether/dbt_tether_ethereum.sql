{{ config(
    alias = 'dbt_tether_ethereum'
    , materialized='incremental'
    , incremental_strategy='delete+insert'
    , partition_by=['dt']
    , unique_key=['dt', 'tx_hash', 'evt_index']
    , enabled = true
) }}

SELECT 
    DATE(evt_block_time) AS dt,
    evt_block_time as period,
    'ethereum' AS blockchain,
    value / power(10, 6) AS amount,
    "from",
    "to",
    evt_tx_hash as tx_hash,
    evt_index
FROM {{ source('tether_ethereum', 'tether_usd_evt_transfer') }}
WHERE contract_address = 0xdac17f958d2ee523a2206206994597c13d831ec7
{% if is_incremental() %}
AND evt_block_time >= NOW() - interval '3' day
{% endif %}