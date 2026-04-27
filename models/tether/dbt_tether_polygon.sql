{{ config(
    alias = 'dbt_tether_polygon'
    , materialized='incremental'
    , incremental_strategy='delete+insert'
    , partition_by=['dt']
    , unique_key=['dt', 'tx_hash', 'evt_index']
    , enabled = true
) }}

SELECT 
    DATE(evt_block_time) AS dt,
    evt_block_time as period,
    'polygon' AS blockchain,
    value / power(10, 6) AS amount,
    "from",
    "to",
    evt_tx_hash as tx_hash,
    evt_index
FROM {{ source('tether_polygon', 'uchilderc20_evt_transfer') }}
WHERE contract_address = 0xc2132d05d31c914a87c6611c10748aeb04b58e8f
{% if is_incremental() %}
AND evt_block_time >= NOW() - interval '3' day
{% endif %}