{{ config(
    alias = 'dbt_tether_tron'
    , materialized='incremental'
    , incremental_strategy='append'
    , partition_by=['period']
    , unique_key=['tx_hash', 'evt_index']
    , enabled = true
) }}

SELECT 
    evt_block_time AS period,
    'tron' AS blockchain,
    value / power(10, 6) AS amount,
    "from",
    "to",
    evt_tx_hash as tx_hash,
    evt_index
FROM {{ source('tether_tron', 'tether_usd_evt_transfer') }}
WHERE contract_address = 0xa614f803b6fd780986a42c78ec9c7f77e6ded13c
{% if is_incremental() %}
AND evt_block_time >= (SELECT MAX(period) FROM {{ this }}) - interval '1' day
{% endif %}