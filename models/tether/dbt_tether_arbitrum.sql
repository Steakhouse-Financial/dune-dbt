{{ config(
    alias = 'dbt_tether_arbitrum'
    , materialized='incremental'
    , incremental_strategy='append'
    , partition_by=['period']
    , unique_key=['tx_hash', 'evt_index']
    , enabled = true
) }}

SELECT 
    evt_block_time AS period,
    'arbitrum' AS blockchain,
    value / power(10, 6) AS amount,
    "from",
    "to",
    evt_tx_hash as tx_hash,
    evt_index
FROM {{ source('tether_arbitrum', 'ArbitrumExtension_evt_Transfer') }}
WHERE contract_address = 0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9
{% if is_incremental() %}
AND evt_block_time >= (SELECT MAX(period) FROM {{ this }}) - interval '1' day
{% endif %}