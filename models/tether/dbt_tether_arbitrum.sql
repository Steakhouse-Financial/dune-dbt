{{ config(
    alias = 'dbt_tether_arbitrum'
    , materialized='incremental'
    , incremental_strategy='delete+insert'
    , partition_by=['dt']
    , unique_key=['dt', 'tx_hash', 'evt_index']
    , enabled = true
) }}

SELECT 
    DATE(evt_block_time) AS dt,
    evt_block_time as period,
    'arbitrum' AS blockchain,
    value / power(10, 6) AS amount,
    "from",
    "to",
    evt_tx_hash as tx_hash,
    evt_index
FROM {{ source('tether_arbitrum', 'ArbitrumExtension_evt_Transfer') }}
WHERE contract_address = 0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9
{% if is_incremental() %}
AND evt_block_time >= NOW() - interval '3' day
{% endif %}