{{ config(
    alias = 'dbt_tether_bnb'
    , materialized='incremental'
    , incremental_strategy='append'
    , partition_by=['period']
    , unique_key=['tx_hash', 'evt_index']
    , enabled = true
) }}

SELECT 
    evt_block_time AS period,
    'bnb' AS blockchain,
    value / power(10, 18) AS amount,
    "from",
    "to",
    evt_tx_hash as tx_hash,
    evt_index
FROM {{ source('bep20usdt_bnb', 'bep20usdt_evt_transfer') }}
WHERE contract_address = 0x55d398326f99059ff775485246999027b3197955
{% if is_incremental() %}
AND evt_block_time >= (SELECT MAX(period) FROM {{ this }}) - interval '1' day
{% endif %}