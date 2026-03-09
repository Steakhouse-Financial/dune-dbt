{{ config(
    alias = 'dbt_tether_ethereum'
    , materialized='incremental'
    , incremental_strategy='append'
    , partition_by=['period']
    , unique_key=['tx_hash', 'evt_index']
    , enabled = true
) }}

SELECT 
    evt_block_time AS period,
    'ethereum' AS blockchain,
    value / power(10, 6) AS amount,
    "from",
    "to",
    evt_tx_hash as tx_hash,
    evt_index
FROM {{ source('tether_ethereum', 'tether_usd_evt_transfer') }}
WHERE contract_address = 0xdac17f958d2ee523a2206206994597c13d831ec7
{% if is_incremental() %}
AND evt_block_time >= (SELECT MAX(period) FROM {{ this }}) - interval '1' day
{% endif %}