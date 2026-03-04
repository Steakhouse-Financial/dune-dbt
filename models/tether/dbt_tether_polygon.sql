{{ config(
    alias = 'dbt_tether_polygon'
    , materialized='incremental'
    , incremental_strategy='append'
    , partition_by=['period']
    , unique_key=['tx_hash', 'evt_index']
    , enabled = true
) }}

SELECT 
    evt_block_time AS period,
    'polygon' AS blockchain,
    value / power(10, 6) AS amount,
    "from",
    "to",
    evt_tx_hash as tx_hash,
    evt_index
FROM {{ source('tether_polygon', 'uchilderc20_evt_transfer') }}
WHERE contract_address = 0xc2132d05d31c914a87c6611c10748aeb04b58e8f
{% if is_incremental() %}
AND evt_block_time >= (SELECT MAX(period) FROM {{ this }}) - interval '1' day
{% endif %}